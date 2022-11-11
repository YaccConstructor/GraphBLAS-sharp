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
    (isValueValid: 'a -> bool)
    case
    (vector: 'a [], mask: 'b [])
    (value: 'a)
    =

    let vectorNNZ =
        NNZCountCount vector (vectorIsZero vectorZero)

    let maskNNZ =
        NNZCountCount mask (maskIsEqual maskZero)

    if vectorNNZ > 0 && maskNNZ > 0 && isValueValid value then
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
        | ex when ex.Message = "InvalidBufferSize" -> ()
        | ex -> raise ex

let testFixtures case =
    let config = defaultConfig

    let getCorrectnessTestName datatype maskFormat =
        $"Correctness on %s{datatype}, vector: %A{case.Format}, mask: %s{maskFormat}"

    let wgSize = 32
    let context = case.ClContext.ClContext

    let floatIsEqual x y =
        abs (x - y) < Accuracy.medium.absolute || x = y

    [ let intFill = Vector.fillSubVector context wgSize

      let intToCoo = Vector.toSparse context wgSize

      case
      |> makeTest (=) (=) 0 0 intToCoo intFill VectorFormat.Sparse (fun item -> true)
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "Sparse")

      let floatFill = Vector.fillSubVector context wgSize

      let floatToCoo = Vector.toSparse context wgSize

      case
      |> makeTest floatIsEqual floatIsEqual 0.0 0.0 floatToCoo floatFill VectorFormat.Sparse System.Double.IsNormal
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "Sparse")

      let byteFill = Vector.fillSubVector context wgSize

      let byteToCoo = Vector.toSparse context wgSize

      case
      |> makeTest (=) (=) 0uy 0uy byteToCoo byteFill VectorFormat.Sparse (fun item -> true)
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "Sparse")

      let boolFill = Vector.fillSubVector context wgSize

      let boolToCoo = Vector.toSparse context wgSize

      case
      |> makeTest (=) (=) false false boolToCoo boolFill VectorFormat.Sparse (fun item -> true)
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "Sparse")

      let intFill = Vector.fillSubVector context wgSize

      let intToCoo = Vector.toSparse context wgSize

      case
      |> makeTest (=) (=) 0 0 intToCoo intFill VectorFormat.Dense (fun item -> true)
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "Dense")

      let floatFill = Vector.fillSubVector context wgSize

      let floatToCoo = Vector.toSparse context wgSize

      case
      |> makeTest floatIsEqual floatIsEqual 0.0 0.0 floatToCoo floatFill VectorFormat.Dense System.Double.IsNormal
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "Dense")

      let byteFill = Vector.fillSubVector context wgSize

      let byteToCoo = Vector.toSparse context wgSize

      case
      |> makeTest (=) (=) 0uy 0uy byteToCoo byteFill VectorFormat.Dense (fun item -> true)
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "Dense")

      let boolFill = Vector.fillSubVector context wgSize

      let boolToCoo = Vector.toSparse context wgSize

      case
      |> makeTest (=) (=) false false boolToCoo boolFill VectorFormat.Dense (fun item -> true)
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "Dense") ]

let tests =
    testsWithOperationCase<VectorFormat> testFixtures "Backend.Vector.fillSubVector tests"
