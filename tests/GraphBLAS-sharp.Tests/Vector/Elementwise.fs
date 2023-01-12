module GraphBLAS.FSharp.Tests.Backend.Vector.Elementwise

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Utils
open TestCases
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Backend.Common.StandardOperations

let logger = Log.create "Vector.ElementWise.Tests"

let config = defaultConfig

let NNZCountCount array isZero =
    Array.filter (fun item -> not <| isZero item) array
    |> Array.length

let checkResult isEqual resultZero (op: 'a -> 'b -> 'c) (actual: Vector<'c>) (leftArray: 'a []) (rightArray: 'b []) =

    let expectedArrayLength = leftArray.Length

    let expectedArray =
        Array.create expectedArrayLength resultZero

    for i in 0 .. expectedArrayLength - 1 do
        expectedArray.[i] <- op leftArray.[i] rightArray.[i]

    let (Vector.Dense expected) =
        createVectorFromArray Dense expectedArray (isEqual resultZero)

    match actual with
    | Vector.Dense actual ->
        "arrays must have the same values"
        |> Expect.equal actual expected
    | _ -> failwith "Vector format must be Sparse."

let correctnessGenericTest
    leftIsEqual
    rightIsEqual
    resultIsEqual
    leftZero
    rightZero
    resultZero
    op
    (addFun: MailboxProcessor<_> -> ClVector<'a> -> ClVector<'b> -> ClVector<'c>)
    (toDense: MailboxProcessor<_> -> ClVector<'c> -> ClVector<'c>)
    case
    (leftArray: 'a [], rightArray: 'b [])
    =

    let leftNNZCount =
        NNZCountCount leftArray (leftIsEqual leftZero)

    let rightNNZCount =
        NNZCountCount rightArray (rightIsEqual rightZero)

    if leftNNZCount > 0 && rightNNZCount > 0 then

        let context = case.TestContext.ClContext
        let q = case.TestContext.Queue

        let firstVector =
            createVectorFromArray case.Format leftArray (leftIsEqual leftZero)

        let secondVector =
            createVectorFromArray case.Format rightArray (rightIsEqual rightZero)

        let v1 = firstVector.ToDevice context
        let v2 = secondVector.ToDevice context

        try
            let res = addFun q v1 v2

            v1.Dispose q
            v2.Dispose q

            let denseActual = toDense q res

            let actual = denseActual.ToHost q

            res.Dispose q
            denseActual.Dispose q

            checkResult resultIsEqual resultZero op actual leftArray rightArray
        with
        | ex when ex.Message = "InvalidBufferSize" -> ()
        | ex -> raise ex

let addTestFixtures case =

    let getCorrectnessTestName fstType sndType thrType =
        $"Correctness on '{fstType} option -> '{sndType} option -> '{thrType} option, {case.Format}"

    let wgSize = 32

    let context = case.TestContext.ClContext

    [ let intAddFun = Vector.elementWise context intSum wgSize

      let intToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0 0 0 (+) intAddFun intToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "int" "int")

      let floatAddFun =
          Vector.elementWise context floatSum wgSize

      let fIsEqual =
          fun x y -> abs (x - y) < Accuracy.medium.absolute || x = y

      let floatToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest fIsEqual fIsEqual fIsEqual 0.0 0.0 0.0 (+) floatAddFun floatToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "float" "float")

      let boolAddFun =
          Vector.elementWise context boolSum wgSize

      let boolToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) (=) (=) false false false (||) boolAddFun boolToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "bool" "bool")

      let byteAddFun =
          Vector.elementWise context byteSum wgSize

      let byteToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0uy 0uy 0uy (+) byteAddFun byteToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "byte" "byte") ]

let addTests =
    operationGPUTests "Backend.Vector.ElementWiseAdd tests" addTestFixtures

let mulTestFixtures case =
    let getCorrectnessTestName fstType sndType thrType =
        $"Correctness on '{fstType} option -> '{sndType} option -> '{thrType} option, {case.Format}"

    let wgSize = 32

    let context = case.TestContext.ClContext

    [ let intMulFun = Vector.elementWise context intMul wgSize

      let intToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0 0 0 (*) intMulFun intToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "int" "int")

      let floatMulFun =
          Vector.elementWise context floatMul wgSize

      let fIsEqual =
          fun x y -> abs (x - y) < Accuracy.medium.absolute || x = y

      let floatToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest fIsEqual fIsEqual fIsEqual 0.0 0.0 0.0 (*) floatMulFun floatToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "float" "float")

      let boolMulFun =
          Vector.elementWise context boolMul wgSize

      let boolToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) (=) (=) false false false (&&) boolMulFun boolToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "bool" "bool")

      let byteMulFun =
          Vector.elementWise context byteMul wgSize

      let byteToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0uy 0uy 0uy (*) byteMulFun byteToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "byte" "byte") ]

let mulTests =
    operationGPUTests "Backend.Vector.ElementWiseMul tests" addTestFixtures
