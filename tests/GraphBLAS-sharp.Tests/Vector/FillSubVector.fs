module Backend.Vector.FillSubVector

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Utils
open Brahma.FSharp
open TestCases

let logger = Log.create "Vector.fillSubVector.Tests"

let clContext = Context.defaultContext.ClContext

let NNZCount array isZero =
    Array.filter (fun item -> not <| isZero item) array
    |> Array.length

let complemented isComplemented value =
    if isComplemented then
        not value
    else
        value

let checkResult
    (resultIsEqual: 'a -> 'a -> bool)
    (maskIsEqual: 'b -> 'b -> bool)
    vectorZero
    maskZero
    isComplemented
    (actual: Vector<'a>)
    (vector: 'a [])
    (mask: 'b [])
    (value: 'a)
    =

    let expectedArray = Array.create vector.Length vectorZero

    let complemented = complemented isComplemented

    for i in 0 .. vector.Length - 1 do
        if complemented (not <| maskIsEqual mask.[i] maskZero) then
            expectedArray.[i] <- value
        else
            expectedArray.[i] <- vector.[i]

    match actual with
    | VectorSparse actual ->
        let actualArray = Array.create vector.Length vectorZero

        for i in 0 .. actual.Indices.Length - 1 do
            actualArray.[actual.Indices.[i]] <- actual.Values.[i]

        "arrays must have the same values and length"
        |> compareArrays resultIsEqual actualArray expectedArray
    | _ -> failwith "Vector format must be Sparse."

let makeTest<'a, 'b when 'a: struct and 'b: struct>
    vectorIsEqual
    maskIsEqual
    vectorZero
    maskZero
    (toCoo: MailboxProcessor<_> -> ClVector<'a> -> ClVector<'a>)
    (fillVector: MailboxProcessor<Msg> -> ClVector<'a> -> ClVector<'b> -> ClCell<'a> -> ClVector<'a>)
    (isValueValid: 'a -> bool)
    isComplemented
    case
    (vector: 'a [], mask: 'b [])
    (value: 'a)
    =

    let vectorNNZ =
        NNZCount vector (vectorIsEqual vectorZero)

    let maskNNZ = NNZCount mask (maskIsEqual maskZero)

    if vectorNNZ > 0 && maskNNZ > 0 && isValueValid value then
        let q = case.ClContext.Queue
        let context = case.ClContext.ClContext

        let leftVector =
            createVectorFromArray case.Format vector (vectorIsEqual vectorZero)

        let maskVector =
            createVectorFromArray case.Format mask (maskIsEqual maskZero)

        let clLeftVector = leftVector.ToDevice context

        let clMaskVector = maskVector.ToDevice context

        try
            let clValue = context.CreateClCell<'a> value

            let clActual =
                fillVector q clLeftVector clMaskVector clValue

            let cooClActual = toCoo q clActual

            let actual = cooClActual.ToHost q

            clLeftVector.Dispose q
            clMaskVector.Dispose q
            clActual.Dispose q
            cooClActual.Dispose q

            checkResult vectorIsEqual maskIsEqual vectorZero maskZero isComplemented actual vector mask value
        with
        | ex when ex.Message = "InvalidBufferSize" -> ()
        | ex -> raise ex

let testFixtures case =
    let config = defaultConfig

    let getCorrectnessTestName datatype =
        $"Correctness on %s{datatype}, vector: %A{case.Format}"

    let wgSize = 32
    let context = case.ClContext.ClContext

    let floatIsEqual x y =
        abs (x - y) < Accuracy.medium.absolute || x = y

    let isComplemented = false

    [ let intFill =
          Vector.standardFillSubVector context wgSize

      let intToCoo = Vector.toSparse context wgSize

      case
      |> makeTest (=) (=) 0 0 intToCoo intFill (fun _ -> true) isComplemented
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let floatFill =
          Vector.standardFillSubVector context wgSize

      let floatToCoo = Vector.toSparse context wgSize

      case
      |> makeTest floatIsEqual floatIsEqual 0.0 0.0 floatToCoo floatFill System.Double.IsNormal isComplemented
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let byteFill =
          Vector.standardFillSubVector context wgSize

      let byteToCoo = Vector.toSparse context wgSize

      case
      |> makeTest (=) (=) 0uy 0uy byteToCoo byteFill (fun _ -> true) isComplemented
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")

      let boolFill =
          Vector.standardFillSubVector context wgSize

      let boolToCoo = Vector.toSparse context wgSize

      case
      |> makeTest (=) (=) false false boolToCoo boolFill (fun _ -> true) isComplemented
      |> testPropertyWithConfig config (getCorrectnessTestName "bool") ]

let tests =
    operationGPUTests "Backend.Vector.fillSubVector tests" testFixtures

let testFixturesComplemented case =
    let config = defaultConfig

    let getCorrectnessTestName datatype =
        $"Correctness on %s{datatype}, vector: %A{case.Format}"

    let wgSize = 32
    let context = case.ClContext.ClContext

    let floatIsEqual x y =
        abs (x - y) < Accuracy.medium.absolute || x = y

    let isComplemented = true

    [ let intFill =
          Vector.standardFillSubVectorComplemented context wgSize

      let intToCoo = Vector.toSparse context wgSize

      case
      |> makeTest (=) (=) 0 0 intToCoo intFill (fun _ -> true) isComplemented
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let floatFill =
          Vector.standardFillSubVectorComplemented context wgSize

      let floatToCoo = Vector.toSparse context wgSize

      case
      |> makeTest floatIsEqual floatIsEqual 0.0 0.0 floatToCoo floatFill System.Double.IsNormal isComplemented
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let byteFill =
          Vector.standardFillSubVectorComplemented context wgSize

      let byteToCoo = Vector.toSparse context wgSize

      case
      |> makeTest (=) (=) 0uy 0uy byteToCoo byteFill (fun _ -> true) isComplemented
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")

      let boolFill =
          Vector.standardFillSubVectorComplemented context wgSize

      let boolToCoo = Vector.toSparse context wgSize

      case
      |> makeTest (=) (=) false false boolToCoo boolFill (fun _ -> true) isComplemented
      |> testPropertyWithConfig config (getCorrectnessTestName "bool") ]

let complementedTests =
    operationGPUTests "Backend.Vector.fillSubVectorComplemented tests" testFixturesComplemented
