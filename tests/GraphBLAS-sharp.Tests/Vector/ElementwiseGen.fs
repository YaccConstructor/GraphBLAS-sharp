module GraphBLAS.FSharp.Tests.Backend.Vector.Sparse.ElementwiseGen

open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Vector.Sparse
open Brahma.FSharp
open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Tests.Utils
open GraphBLAS.FSharp.Backend.Quotes.ArithmeticOperations
open TestCases
open GraphBLAS.FSharp.Tests.Backend.Vector.Elementwise
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Objects

let logger =
    Log.create "Backend.Vector.SparseVector.ElementWiseGen.Tests"

let config = defaultConfig

let correctnessGenericTest
    leftIsEqual
    rightIsEqual
    resultIsEqual
    leftZero
    rightZero
    resultZero
    op
    (addFun: MailboxProcessor<_> -> ClVector.Sparse<'a> -> ClVector.Sparse<'b> -> ClVector.Sparse<'c>)
    (toDense: MailboxProcessor<_> -> ClVector.Sparse<'c> -> ClArray<'c option>)
    (case: OperationCase<VectorFormat>)
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

        let (ClVector.Sparse v1) = firstVector.ToDevice context
        let (ClVector.Sparse v2) = secondVector.ToDevice context

        try
            let res = addFun q v1 v2

            v1.Dispose q
            v2.Dispose q

            let denseActual = toDense q res

            let actual = denseActual.ToHost q

            res.Dispose q
            denseActual.Dispose q

            checkResult resultIsEqual resultZero op (Vector.Dense actual) leftArray rightArray
        with
        | ex when ex.Message = "InvalidBufferSize" -> ()
        | ex -> raise ex

let addTestFixtures (testContext: TestContext) =
    let wgSize = 32

    let context = testContext.ClContext

    let case =
        { TestContext = testContext
          Format = Sparse }

    let getCorrectnessTestName = getCorrectnessTestName case

    [ let intAddFun =
          SparseVector.elementwiseGen context intSum wgSize

      let intToDense = SparseVector.toDense context wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0 0 0 (+) intAddFun intToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "int" "int")

      let floatAddFun =
          SparseVector.elementwiseGen context floatSum wgSize

      let fIsEqual =
          fun x y -> abs (x - y) < Accuracy.medium.absolute || x = y

      let floatToDense = SparseVector.toDense context wgSize

      case
      |> correctnessGenericTest fIsEqual fIsEqual fIsEqual 0.0 0.0 0.0 (+) floatAddFun floatToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "float" "float")

      let boolAddFun =
          SparseVector.elementwiseGen context boolSum wgSize

      let boolToDense = SparseVector.toDense context wgSize

      case
      |> correctnessGenericTest (=) (=) (=) false false false (||) boolAddFun boolToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "bool" "bool")

      let byteAddFun =
          SparseVector.elementwiseGen context byteSum wgSize

      let byteToDense = SparseVector.toDense context wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0uy 0uy 0uy (+) byteAddFun byteToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "byte" "byte") ]

let addTests =
    gpuTests "Backend.Vector.ElementWiseAddGen tests" addTestFixtures

let fillSubVectorComplementedQ<'a, 'b> value =
    <@ fun (left: 'a option) (right: 'b option) ->
        match left with
        | None -> Some value
        | _ -> right @>

let fillSubVectorFun value zero isEqual =
    fun left right ->
        if isEqual left zero then
            value
        else
            right

let mulTestFixtures (testContext: TestContext) =
    let wgSize = 32

    let context = testContext.ClContext

    let case =
        { TestContext = testContext
          Format = Sparse }

    let getCorrectnessTestName = getCorrectnessTestName case

    [ let intMulFun =
          SparseVector.elementwiseGen context intMul wgSize

      let intToDense = SparseVector.toDense context wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0 0 0 (*) intMulFun intToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "int" "int")

      let floatMulFun =
          SparseVector.elementwiseGen context floatMul wgSize

      let fIsEqual =
          fun x y -> abs (x - y) < Accuracy.medium.absolute || x = y

      let floatToDense = SparseVector.toDense context wgSize

      case
      |> correctnessGenericTest fIsEqual fIsEqual fIsEqual 0.0 0.0 0.0 (*) floatMulFun floatToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "float" "float")

      let boolMulFun =
          SparseVector.elementwiseGen context boolMul wgSize

      let boolToDense = SparseVector.toDense context wgSize

      case
      |> correctnessGenericTest (=) (=) (=) false false false (&&) boolMulFun boolToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "bool" "bool")

      let byteMulFun =
          SparseVector.elementwiseGen context byteMul wgSize

      let byteToDense = SparseVector.toDense context wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0uy 0uy 0uy (*) byteMulFun byteToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "byte" "byte") ]

let mulTests =
    gpuTests "Backend.Vector.SparseVector.ElementWiseMulGen tests" mulTestFixtures

let noneNoneTestFixtures (testContext: TestContext) =
    let wgSize = 32

    let context = testContext.ClContext

    let case =
        { TestContext = testContext
          Format = Sparse }

    let getCorrectnessTestName = getCorrectnessTestName case

    [ let intMaskFun =
          SparseVector.elementwiseGen context (fillSubVectorComplementedQ 1) wgSize

      let intToDense = SparseVector.toDense context wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0 0 0 (fillSubVectorFun 1 0 (=)) intMaskFun intToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "int" "int")

      let floatMaskFun =
          SparseVector.elementwiseGen context (fillSubVectorComplementedQ 1.0) wgSize

      let fIsEqual =
          fun x y -> abs (x - y) < Accuracy.medium.absolute || x = y

      let floatToDense = SparseVector.toDense context wgSize

      case
      |> correctnessGenericTest
          fIsEqual
          fIsEqual
          fIsEqual
          0.0
          0.0
          0.0
          (fillSubVectorFun 1.0 0.0 fIsEqual)
          floatMaskFun
          floatToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "float" "float")

      let boolMaskFun =
          SparseVector.elementwiseGen context (fillSubVectorComplementedQ true) wgSize

      let boolToDense = SparseVector.toDense context wgSize

      case
      |> correctnessGenericTest (=) (=) (=) false false false (fillSubVectorFun true false (=)) boolMaskFun boolToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "bool" "bool")

      let byteMaskFun =
          SparseVector.elementwiseGen context (fillSubVectorComplementedQ 1uy) wgSize

      let byteToDense = SparseVector.toDense context wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0uy 0uy 0uy (fillSubVectorFun 1uy 0uy (=)) byteMaskFun byteToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "byte" "byte") ]

let complementedMaskTests =
    gpuTests "Backend.Vector.ElementWiseGen mask tests" noneNoneTestFixtures
