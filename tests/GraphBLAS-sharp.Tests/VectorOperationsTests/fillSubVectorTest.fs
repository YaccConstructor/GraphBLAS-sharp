module Backend.Vector.FillSubVector

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests.Utils

let logger = Log.create "Vector.zeroCreate.Tests"

let clContext = defaultContext.ClContext

let vectorFilter vector isZero =
    Array.filter
    <| (fun item -> not <| isZero item)
    <| vector

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

    let expectedArrayLength =
        max vector.Length mask.Length

    let isVectorLess =
        vector.Length < mask.Length

    let lowBound =
        if isVectorLess then vector.Length else mask.Length

    let expectedArray =
        Array.create expectedArrayLength vectorZero

    for i in 0 .. expectedArrayLength - 1 do
        if i < mask.Length && not (maskIsEqual mask[i] maskZero) then
            expectedArray[i] <- value
        elif i < vector.Length then
            expectedArray[i] <- vector[i]

    match actual with
    | VectorCOO actual ->
        let actualArray = Array.create expectedArrayLength vectorZero

        for i in 0 .. actual.Indices.Length - 1 do
            actualArray[actual.Indices[i]] <- actual.Values[i]

        "arrays must have the same values and length"
        |> compareArrays resultIsEqual actualArray expectedArray
    | _ -> failwith "Vector format must be COO."

let makeTest<'a, 'b when 'a: struct and 'b: struct>
    vectorIsZero
    maskIsEqual
    (vectorZero: 'a )
    (maskZero: 'b)
    (toCoo: MailboxProcessor<_> -> ClVector<'a> -> ClVector<'a>)
    (fillVector: MailboxProcessor<_> -> ClVector<'a> -> ClVector<'b> -> 'a -> ClVector<'a>)
    (maskFormat: VectorFormat)
    case
    (vector: 'a [])
    (mask: 'b [])
    (value: 'a)
    =

    let filteredLeftVector =
        vectorFilter vector (vectorIsZero vectorZero)

    let filteredMask =
        vectorFilter mask (maskIsEqual maskZero)

    if filteredLeftVector.Length > 0 && filteredMask.Length > 0 && not (vectorIsZero value vectorZero) then
        let q = case.ClContext.Queue
        let context = case.ClContext.ClContext

        let leftVector =
            createVectorFromArray case.FormatCase vector (vectorIsZero vectorZero)

        let maskVector =
            createVectorFromArray maskFormat mask (maskIsEqual maskZero)

        let clLeftVector =
            leftVector.ToDevice context

        let clMaskVector =
            maskVector.ToDevice context

        let clActual =
            fillVector q clLeftVector clMaskVector value

        let cooClActual = toCoo q clActual

        let actual = cooClActual.ToHost q

        clLeftVector.Dispose q
        clMaskVector.Dispose q
        clActual.Dispose q
        cooClActual.Dispose q

        checkResult vectorIsZero maskIsEqual vectorZero maskZero actual vector mask value

let testFixtures case =
    let config = defaultConfig

    let getCorrectnessTestName datatype maskFormat =
         $"Correctness on %s{datatype}, vector: %A{case.FormatCase}, mask: %s{maskFormat}"

    let wgSize = 32
    let context = case.ClContext.ClContext

    let floatIsEqual x y =
        abs (x - y) < Accuracy.medium.absolute

    [ let intFill = Vector.fillSubVector context wgSize 0

      let intToCoo = Vector.toCoo context wgSize

      case
      |> makeTest (=) (=) 0 0 intToCoo intFill VectorFormat.COO
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "COO")

      let floatFill = Vector.fillSubVector context wgSize 0.0

      let floatToCoo = Vector.toCoo context wgSize

      case
      |> makeTest floatIsEqual floatIsEqual 0.0 0.0 floatToCoo floatFill VectorFormat.COO
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "COO") //TODO filt floats

      let byteFill = Vector.fillSubVector context wgSize 0uy

      let byteToCoo = Vector.toCoo context wgSize

      case
      |> makeTest (=) (=) 0uy 0uy byteToCoo byteFill VectorFormat.COO
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "COO")

      let boolFill = Vector.fillSubVector context wgSize false

      let boolToCoo = Vector.toCoo context wgSize

      case
      |> makeTest (=) (=) false false boolToCoo boolFill VectorFormat.COO
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "COO")

      let intFill = Vector.fillSubVector context wgSize 0

      let intToCoo = Vector.toCoo context wgSize

      case
      |> makeTest (=) (=) 0 0 intToCoo intFill VectorFormat.Dense
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "Dense")

      let floatFill = Vector.fillSubVector context wgSize 0.0

      let floatToCoo = Vector.toCoo context wgSize

      case
      |> makeTest floatIsEqual floatIsEqual 0.0 0.0 floatToCoo floatFill VectorFormat.Dense
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "Dense") //TODO filt floats

      let byteFill = Vector.fillSubVector context wgSize 0uy

      let byteToCoo = Vector.toCoo context wgSize

      case
      |> makeTest (=) (=) 0uy 0uy byteToCoo byteFill VectorFormat.Dense
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "Dense")

      let boolFill = Vector.fillSubVector context wgSize false

      let boolToCoo = Vector.toCoo context wgSize

      case
      |> makeTest (=) (=) false false boolToCoo boolFill VectorFormat.Dense
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "Dense") ]

let tests =
     testCases<VectorFormat>
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.FormatCase)
    |> List.collect testFixtures
    |> testList "Backend.Vector.fillSubVector tests"
