module Backend.Vector.Complemented

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests.Utils
open OpenCL.Net

let logger = Log.create "Vector.complemented.Tests"

let NNZCountCount array isZero =
    Array.filter (fun item -> not <| isZero item) array
    |> Array.length

let fFilter =
    fun item ->
        System.Double.IsNaN item
        || System.Double.IsInfinity item
    >> not
    |> Array.filter

let checkResult isEqual zero (actual: Vector<'a>) (vector: 'a []) =

    let expectedArrayLength = vector.Length

    let expectedArray = Array.create expectedArrayLength 1

    for i in 0 .. expectedArrayLength - 1 do
        if not <| isEqual vector.[i] zero then
            expectedArray.[i] <- 0

    match actual with
    | VectorSparse actual ->
        let actualArray = Array.create expectedArrayLength 0

        for i in 0 .. actual.Indices.Length - 1 do
            actualArray.[actual.Indices.[i]] <- 1

        $"arrays must have the same values and length"
        |> compareArrays (=) actualArray expectedArray
    | _ -> failwith "Vector format must be Sparse."

let correctnessGenericTest
    isEqual
    zero
    (complemented: MailboxProcessor<_> -> ClVector<'a> -> ClVector<'a>)
    (toCoo: MailboxProcessor<_> -> ClVector<'a> -> ClVector<'a>)
    filter
    case
    (maskArray: 'a [])
    =

    let maskArray = filter maskArray

    let maskNNZ = NNZCountCount maskArray (isEqual zero)

    if maskNNZ > 0 && maskNNZ < maskArray.Length - 1 then
        let q = case.ClContext.Queue
        let context = case.ClContext.ClContext

        let secondVector =
            createVectorFromArray case.Format maskArray (isEqual zero)

        let clVector = secondVector.ToDevice context

        let res = complemented q clVector

        clVector.Dispose q

        let cooRes = toCoo q res

        res.Dispose q

        let actual = cooRes.ToHost q

        cooRes.Dispose q

        checkResult isEqual zero actual maskArray

let testFixtures (case: OperationCase<VectorFormat>) =
    let config = defaultConfig

    let getCorrectnessTestName dataType =
        $"Correctness on %A{dataType}, %A{case.Format}"

    let wgSize = 32
    let context = case.ClContext.ClContext

    [ let intToCoo = Vector.toCoo context wgSize

      let intComplemented = Vector.complemented context wgSize

      case
      |> correctnessGenericTest (=) 0 intComplemented intToCoo id
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let byteToCoo = Vector.toCoo context wgSize

      let byteComplemented = Vector.complemented context wgSize

      case
      |> correctnessGenericTest (=) 0uy byteComplemented byteToCoo id
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")

      let floatToCoo = Vector.toCoo context wgSize

      let floatComplemented = Vector.complemented context wgSize

      case
      |> correctnessGenericTest (=) 0.0 floatComplemented floatToCoo fFilter
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let boolToCoo = Vector.toCoo context wgSize

      let boolComplemented = Vector.complemented context wgSize

      case
      |> correctnessGenericTest (=) false boolComplemented boolToCoo id
      |> testPropertyWithConfig config (getCorrectnessTestName "bool") ]

let tests =
    testCases<VectorFormat>
    |> List.filter
        (fun case ->
            let mutable e = ErrorCode.Unknown
            let device = case.ClContext.ClContext.ClDevice.Device

            let deviceType =
                Cl
                    .GetDeviceInfo(device, DeviceInfo.Type, &e)
                    .CastTo<DeviceType>()

            deviceType = DeviceType.Gpu)
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.Format)
    |> List.collect testFixtures
    |> testList "Backend.Vector.complemented tests"
