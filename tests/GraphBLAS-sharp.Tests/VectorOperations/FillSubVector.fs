module Backend.Vector.FillSubVector

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests.Utils
open Brahma.FSharp
open OpenCL.Net

let logger = Log.create "Vector.fillSubVector.Tests"

let clContext = defaultContext.ClContext

let NNZCountCount array isZero =
    Array.filter (fun item -> not <| isZero item) array
    |> Array.length

let fFilter =
    fun item ->
        System.Double.IsNaN item
        || System.Double.IsInfinity item
    >> not
    |> Array.filter

let checkResult
    (resultIsEqual: 'a -> 'a -> bool)
    (maskIsEqual: 'b -> 'b -> bool)
    vectorZero
    maskZero
    (actual: Vector<'a>)
    (vector: 'a [])
    (mask: 'b [])
    (value: 'a)
    =

    let expectedArrayLength = max vector.Length mask.Length

    let expectedArray =
        Array.create expectedArrayLength vectorZero

    for i in 0 .. expectedArrayLength - 1 do
        if i < mask.Length
           && not <| maskIsEqual mask.[i] maskZero then
            expectedArray.[i] <- value
        elif i < vector.Length then
            expectedArray.[i] <- vector.[i]

    match actual with
    | VectorSparse actual ->
        let actualArray =
            Array.create expectedArrayLength vectorZero

        for i in 0 .. actual.Indices.Length - 1 do
            actualArray.[actual.Indices.[i]] <- actual.Values.[i]

        "arrays must have the same values and length"
        |> compareArrays resultIsEqual actualArray expectedArray
    | _ -> failwith "Vector format must be Sparse."

let makeTest<'a, 'b when 'a: struct and 'b: struct>
    vectorIsZero
    maskIsEqual
    vectorZero
    maskZero
    (toCoo: MailboxProcessor<_> -> ClVector<'a> -> ClVector<'a>)
    (fillVector: MailboxProcessor<Msg> -> ClVector<'a> -> ClVector<'b> -> 'a -> ClVector<'a>)
    (maskFormat: VectorFormat)
    vectorFilter
    maskFilter
    case
    (vector: 'a [])
    (mask: 'b [])
    (value: 'a)
    =

    let vector = vectorFilter vector

    let mask = maskFilter mask

    let vectorNNZ =
        NNZCountCount vector (vectorIsZero vectorZero)

    let maskNNZ =
        NNZCountCount mask (maskIsEqual maskZero)

    let valueNNZCount =
        Array.create 1 value
        |> vectorFilter
        |> Array.length

    if vectorNNZ > 0 && maskNNZ > 0 && valueNNZCount > 0 then
        let q = case.ClContext.Queue
        let context = case.ClContext.ClContext

        let leftVector =
            createVectorFromArray case.Format vector (vectorIsZero vectorZero)

        let maskVector =
            createVectorFromArray maskFormat mask (maskIsEqual maskZero)

        let clLeftVector = leftVector.ToDevice context

        let clMaskVector = maskVector.ToDevice context

        try
            let clActual =
                fillVector q clLeftVector clMaskVector value

            let cooClActual = toCoo q clActual

            let actual = cooClActual.ToHost q

            clLeftVector.Dispose q
            clMaskVector.Dispose q
            clActual.Dispose q
            cooClActual.Dispose q

            checkResult vectorIsZero maskIsEqual vectorZero maskZero actual vector mask value
        with
        | :? OpenCL.Net.Cl.Exception as ex -> logger.debug (eventX $"exception: {ex.Message}")

let testFixtures case =
    let config = defaultConfig

    let getCorrectnessTestName datatype maskFormat =
        $"Correctness on %s{datatype}, vector: %A{case.Format}, mask: %s{maskFormat}"

    let wgSize = 32
    let context = case.ClContext.ClContext

    let floatIsEqual x y =
        abs (x - y) < Accuracy.medium.absolute || x = y

    [ let intFill = Vector.fillSubVector context wgSize

      let intToCoo = Vector.toCoo context wgSize

      case
      |> makeTest (=) (=) 0 0 intToCoo intFill VectorFormat.Sparse id id
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "Sparse")

      let floatFill = Vector.fillSubVector context wgSize

      let floatToCoo = Vector.toCoo context wgSize

      case
      |> makeTest floatIsEqual floatIsEqual 0.0 0.0 floatToCoo floatFill VectorFormat.Sparse fFilter fFilter
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "Sparse")

      let byteFill = Vector.fillSubVector context wgSize

      let byteToCoo = Vector.toCoo context wgSize

      case
      |> makeTest (=) (=) 0uy 0uy byteToCoo byteFill VectorFormat.Sparse id id
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "Sparse")

      let boolFill = Vector.fillSubVector context wgSize

      let boolToCoo = Vector.toCoo context wgSize

      case
      |> makeTest (=) (=) false false boolToCoo boolFill VectorFormat.Sparse id id
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "Sparse")

      let intFill = Vector.fillSubVector context wgSize

      let intToCoo = Vector.toCoo context wgSize

      case
      |> makeTest (=) (=) 0 0 intToCoo intFill VectorFormat.Dense id id
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "Dense")

      let floatFill = Vector.fillSubVector context wgSize

      let floatToCoo = Vector.toCoo context wgSize

      case
      |> makeTest floatIsEqual floatIsEqual 0.0 0.0 floatToCoo floatFill VectorFormat.Dense fFilter fFilter
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "Dense")

      let byteFill = Vector.fillSubVector context wgSize

      let byteToCoo = Vector.toCoo context wgSize

      case
      |> makeTest (=) (=) 0uy 0uy byteToCoo byteFill VectorFormat.Dense id id
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "Dense")

      let boolFill = Vector.fillSubVector context wgSize

      let boolToCoo = Vector.toCoo context wgSize

      case
      |> makeTest (=) (=) false false boolToCoo boolFill VectorFormat.Dense id id
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "Dense") ]

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
    |> testList "Backend.Vector.fillSubVector tests"
