module Backend.Vector.OfList

open Expecto
open Expecto.Logging
open Brahma.FSharp
open GraphBLAS.FSharp.Tests.Utils
open GraphBLAS.FSharp.Backend
open OpenCL.Net

let logger = Log.create "Vector.ofList.Tests"

let filter elements =
    List.filter
    <| (fun item -> fst item > 0)
    <| elements
    |> List.distinctBy fst

let checkResult (isEqual: 'a -> 'a -> bool) (expectedIndices: int []) (expectedValues: 'a []) (actual: Vector<'a>) =

    Expect.equal actual.Size (Array.max expectedIndices + 1) "lengths must be the same"

    match actual with
    | VectorCOO actual ->
        compareArrays (=) actual.Indices expectedIndices "indices must be the same"
        compareArrays isEqual actual.Values expectedValues "values must be the same"
    | _ -> failwith "Vector format must be COO."

let correctnessGenericTest<'a when 'a: struct>
    (isEqual: 'a -> 'a -> bool)
    (ofList: (int * 'a) list -> MailboxProcessor<Msg> -> VectorFormat -> ClVector<'a>)
    (toCoo: MailboxProcessor<_> -> ClVector<'a> -> ClVector<'a>)
    (case: OperationCase<VectorFormat>)
    (elements: (int * 'a) list)
    =

    let elements = filter elements

    if elements.Length > 0 then

        let q = case.ClContext.Queue

        let indices, values =
            elements
            |> Array.ofList
            |> Array.sortBy fst
            |> Array.unzip

        let clActual = ofList elements q case.FormatCase

        let clCooActual = toCoo q clActual

        let actual = clCooActual.ToHost q

        clActual.Dispose q
        clCooActual.Dispose q

        checkResult isEqual indices values actual

let testFixtures (case: OperationCase<VectorFormat>) =
    [ let config = defaultConfig

      let wgSize = 32

      let context = case.ClContext.ClContext
      let q = case.ClContext.Queue

      q.Error.Add(fun e -> failwithf $"%A{e}")

      let getCorrectnessTestName datatype =
          sprintf "Correctness on %s, %A" datatype case.FormatCase

      let boolOfList = Vector.ofList context wgSize

      let toCoo = Vector.toCoo context wgSize

      case
      |> correctnessGenericTest<bool> (=) boolOfList toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      let intOfList = Vector.ofList context wgSize

      let toCoo = Vector.toCoo context wgSize

      case
      |> correctnessGenericTest<int> (=) intOfList toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "int")


      let byteOfList = Vector.ofList context wgSize

      let toCoo = Vector.toCoo context wgSize

      case
      |> correctnessGenericTest<byte> (=) byteOfList toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")

      let floatOfList = Vector.ofList context wgSize

      let toCoo = Vector.toCoo context wgSize

      case
      |> correctnessGenericTest<byte> (=) floatOfList toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "float") ]

let tests =
    testCases
    |> List.filter
        (fun case ->
            let mutable e = ErrorCode.Unknown
            let device = case.ClContext.ClContext.ClDevice.Device

            let deviceType =
                Cl
                    .GetDeviceInfo(device, DeviceInfo.Type, &e)
                    .CastTo<DeviceType>()

            deviceType = DeviceType.Gpu)
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.FormatCase)
    |> List.collect testFixtures
    |> testList "Backend.Vector.ofList tests"
