module Backend.Vector.ZeroCreate

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests.Utils
open OpenCL.Net

let logger = Log.create "Vector.zeroCreate.Tests"

let checkResult size (actual: Vector<'a>) =
    match actual with
    | VectorDense vector ->
        Expect.equal actual.Size size "The size should be the same"

        Array.iter
        <| (fun item -> Expect.equal item None "values must be None")
        <| vector
    | VectorCOO vector ->
        Expect.equal actual.Size 0 "The size should be the 0"

        Expect.equal vector.Values [| Unchecked.defaultof<'a> |] "The values array must contain the default value"
        Expect.equal vector.Indices [| 0 |] "The index array must contain the 0"

let correctnessGenericTest<'a when 'a: struct and 'a: equality>
    (zeroCreate: MailboxProcessor<_> -> int -> VectorFormat -> ClVector<'a>)
    (case: OperationCase<VectorFormat>)
    (vectorSize: int)
    =

    if vectorSize > 0 then
        let q = case.ClContext.Queue

        let (clVector: ClVector<'a>) = zeroCreate q vectorSize case.FormatCase

        let hostVector = clVector.ToHost q

        clVector.Dispose q

        checkResult vectorSize hostVector

let testFixtures (case: OperationCase<VectorFormat>) =
    let config = defaultConfig

    let getCorrectnessTestName dataType =
        $"Correctness on %A{dataType}, %A{case.FormatCase}"

    let wgSize = 32
    let context = case.ClContext.ClContext

    let q = case.ClContext.Queue

    q.Error.Add(fun e -> failwithf "%A" e)

    [ let intZeroCreate = Vector.zeroCreate context wgSize

      case
      |> correctnessGenericTest intZeroCreate
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let byteZeroCreat = Vector.zeroCreate context wgSize

      case
      |> correctnessGenericTest byteZeroCreat
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")


      let floatZeroCreate = Vector.zeroCreate context wgSize

      case
      |> correctnessGenericTest floatZeroCreate
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let boolZeroCreate = Vector.zeroCreate context wgSize

      case
      |> correctnessGenericTest boolZeroCreate
      |> testPropertyWithConfig config (getCorrectnessTestName "bool") ]

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
    |> testList "Backend.Vector.zeroCreate tests"
