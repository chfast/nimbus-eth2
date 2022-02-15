import
  std/os,
  chronicles,
  stew/results,
  snappy/framing,
  ../ncli/e2store,
  ./spec/datatypes/[altair, bellatrix, phase0],
  ./spec/forks

export results, forks, e2store

type
  EraFile = ref object
    handle: IoHandle
    stateIdx: Index
    blockIdx: Index

  EraDB* = ref object
    cfg: RuntimeConfig
    path: string

    files: seq[EraFile]

proc era(s: Slot): uint64 =
  uint64(s) div SLOTS_PER_HISTORICAL_ROOT

proc start_slot(era: uint64): Slot =
  Slot(era * SLOTS_PER_HISTORICAL_ROOT)

proc getEraFile(
    db: EraDB, state: ForkyBeaconState, era: uint64): Result[EraFile, string] =
  for f in db.files:
    if f.stateIdx.startSlot.era == era:
      return ok(f)

  if db.files.len > 16:
    discard closeFile(db.files[0].handle)
    db.files.delete(0)

  if era >= state.historical_roots.lenu64():
    return err("Era outside of known history")

  let
    name = eraFileName(db.cfg, state, era)
  var
    f = Opt[IoHandle].ok(? openFile(db.path / name, {OpenFlags.Read}).mapErr(ioErrorMsg))

  defer:
    if f.isSome(): discard closeFile(f[])

  # Indices can be found at the end of each era file - we only support
  # single-era files for now
  ? f[].setFilePos(0, SeekPosition.SeekEnd).mapErr(ioErrorMsg)

  # Last in the file is the state index
  let
    stateIdxPos = ? f[].findIndexStartOffset()
  ? f[].setFilePos(stateIdxPos, SeekPosition.SeekCurrent).mapErr(ioErrorMsg)

  let
    stateIdx = ? f[].readIndex()
  if stateIdx.offsets.len() != 1:
    return err("State index length invalid")

  ? f[].setFilePos(stateIdxPos, SeekPosition.SeekCurrent).mapErr(ioErrorMsg)

  # The genesis era file does not contain a block index
  let blockIdx = if stateIdx.startSlot > 0:
    let
      blockIdxPos = ? f[].findIndexStartOffset()
    ? f[].setFilePos(blockIdxPos, SeekPosition.SeekCurrent).mapErr(ioErrorMsg)
    let idx = ? f[].readIndex()
    if idx.offsets.lenu64() != SLOTS_PER_HISTORICAL_ROOT:
      return err("Block index length invalid")

    idx
  else:
    Index()

  let res = EraFile(handle: f[], stateIdx: stateIdx, blockIdx: blockIdx)
  reset(f)

  db.files.add(res)
  ok(res)

proc getBlockSZ*(
    db: EraDB, state: ForkyBeaconState, slot: Slot, bytes: var seq[byte]):
    Result[void, string] =
  ## Get a snappy-frame-compressed version of the block data - may overwrite
  ## `bytes` on error

  # Block content for the blocks of an era is found in the file for the _next_
  # era
  let
    f = ? db.getEraFile(state, slot.era + 1)
    pos = f[].blockIdx.offsets[slot - f[].blockIdx.startSlot]

  if pos == 0:
    return err("No block at given slot")

  ? f.handle.setFilePos(pos, SeekPosition.SeekBegin).mapErr(ioErrorMsg)

  let header = ? f.handle.readRecord(bytes)
  if header.typ != SnappyBeaconBlock:
    return err("Invalid era file: didn't find block at index position")

  ok()

proc getBlockSSZ*(
    db: EraDB, state: ForkyBeaconState, slot: Slot, bytes: var seq[byte]):
    Result[void, string] =
  var tmp: seq[byte]
  ? db.getBlockSZ(state, slot, tmp)

  try:
    bytes = framingFormatUncompress(tmp)
    ok()
  except CatchableError as exc:
    err(exc.msg)

proc getBlock*[T: ForkyTrustedSignedBeaconBlock](
    db: EraDB, state: ForkyBeaconState, slot: Slot,
    root: Opt[Eth2Digest]): Opt[T] =
  var tmp: seq[byte]
  ? db.getBlockSSZ(state, slot, tmp).mapErr(proc(x: auto) = discard)

  result.ok(default(T))
  try:
    readSszBytes(tmp, result.get(), updateRoot = root.isNone)
    if root.isSome():
      result.get().root = root.get()
  except CatchableError as exc:
    result.err()

proc getStateSZ*(
    db: EraDB, state: ForkyBeaconState, slot: Slot, bytes: var seq[byte]):
    Result[void, string] =
  ## Get a snappy-frame-compressed version of the state data - may overwrite
  ## `bytes` on error

  # Block content for the blocks of an era is found in the file for the _next_
  # era
  let
    f = ? db.getEraFile(state, slot.era)

  if f.stateIdx.startSlot != slot:
    return err("State not found in era file")

  let pos = f.stateIdx.offsets[0]
  if pos == 0:
    return err("No state at given slot")

  ? f.handle.setFilePos(pos, SeekPosition.SeekBegin).mapErr(ioErrorMsg)

  let header = ? f.handle.readRecord(bytes)
  if header.typ != SnappyBeaconState:
    return err("Invalid era file: didn't find state at index position")

  ok()

proc getStateSSZ*(
    db: EraDB, state: ForkyBeaconState, slot: Slot, bytes: var seq[byte]):
    Result[void, string] =
  var tmp: seq[byte]
  ? db.getStateSZ(state, slot, tmp)

  try:
    bytes = framingFormatUncompress(tmp)
    ok()
  except CatchableError as exc:
    err(exc.msg)

proc getState[T: ForkyBeaconState](
    db: EraDB, state: ForkyBeaconState, slot: Slot, output: var T): bool =
  # TODO add rollback, export
  var tmp: seq[byte]
  if db.getStateSSZ(state, slot, tmp).isErr:
    return false

  try:
    readSszBytes(tmp, output)
    true
  except CatchableError as exc:
    # TODO log?
    false

iterator getSummaries*(
    db: EraDB, state: ptr ForkyBeaconState, era: uint64): tuple[slot: Slot, root: Eth2Digest] =
  # TODO partially load the state up to the root field
  # TODO maybe reuse BeaconBlockSummary type
  # TODO state: ptr https://github.com/nim-lang/Nim/issues/18188
  var
    roots: array[SLOTS_PER_HISTORICAL_ROOT, Eth2Digest]
    i_love_iterators = false
  case db.cfg.stateForkAtEpoch(era.start_slot.epoch)
  of BeaconStateFork.Phase0:
    var tmp = (ref phase0.BeaconState)()
    if getState(db, state[], era.start_slot(), tmp[]):
      roots = tmp[].block_roots.data
      i_love_iterators = true
  of BeaconStateFork.Altair:
    var tmp = (ref altair.BeaconState)()
    if getState(db, state[], era.start_slot(), tmp[]):
      roots = tmp[].block_roots.data
      i_love_iterators = true
  of BeaconStateFork.Bellatrix:
    var tmp = (ref bellatrix.BeaconState)()
    if getState(db, state[], era.start_slot(), tmp[]):
      roots = tmp[].block_roots.data
      i_love_iterators = true

  var
    lastRoot: Eth2Digest
    slot = era.start_slot() - roots.lenu64()
  for root in roots:
    if root != lastRoot:
      yield (slot, root)
      lastRoot = root
    slot += 1

proc new*(T: type EraDB, cfg: RuntimeConfig, path: string): EraDB =
  EraDb(cfg: cfg, path: path)
