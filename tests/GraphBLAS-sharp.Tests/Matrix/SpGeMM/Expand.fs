module GraphBLAS.FSharp.Tests.Matrix.SpGeMM.Expand

open Expecto
open GraphBLAS.FSharp.Backend.Matrix.CSR.SpGeMM
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Test
open GraphBLAS.FSharp.Tests.Context
open Microsoft.FSharp.Collections
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Backend
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Objects.MatrixExtensions

let config =
    { Utils.defaultConfig with
          arbitrary = [ typeof<Generators.PairOfMatricesOfCompatibleSize> ]
          endSize = 100
          maxTest = 100 }

let createCSRMatrix array isZero =
    Utils.createMatrixFromArray2D CSR array isZero
    |> Utils.castMatrixToCSR

let getSegmentsPointers (leftMatrix: Matrix.CSR<'a>) (rightMatrix: Matrix.CSR<'b>) =
    Array.map
        (fun item ->
            rightMatrix.RowPointers.[item + 1]
            - rightMatrix.RowPointers.[item])
        leftMatrix.ColumnIndices
    |> HostPrimitives.prefixSumExclude 0 (+)

let makeTest (testContext: TestContext) isZero testFun (leftArray: 'a [,], rightArray: 'a [,]) =
    let context = testContext.ClContext
    let processor = testContext.Queue

    let leftMatrix = createCSRMatrix leftArray isZero

    let rightMatrix = createCSRMatrix rightArray isZero

    let expectedPointers, expectedLength =
        getSegmentsPointers leftMatrix rightMatrix

    if leftMatrix.NNZ > 0
       && rightMatrix.NNZ > 0
       && expectedLength > 0 then
        let clLeftMatrix = leftMatrix.ToDevice context

        let clRightMatrix = rightMatrix.ToDevice context

        let actualLength, (clActual: ClArray<int>) =
            testFun processor clLeftMatrix clRightMatrix

        clLeftMatrix.Dispose processor
        clRightMatrix.Dispose processor

        let actualPointers = clActual.ToHostAndFree processor

        "Results lengths must be the same"
        |> Expect.equal actualLength expectedLength

        "Result pointers must be the same"
        |> Expect.sequenceEqual actualPointers expectedPointers

let createTest<'a when 'a: struct> (testContext: TestContext) (isZero: 'a -> bool) =

    let testFun =
        Expand.getSegmentPointers testContext.ClContext Utils.defaultWorkGroupSize

    makeTest testContext isZero testFun
    |> testPropertyWithConfig config $"test on {typeof<'a>}"

let getSegmentsTests (testContext: TestContext) =
    [ createTest testContext ((=) 0)

      if Utils.isFloat64Available testContext.ClContext.ClDevice then
          createTest testContext ((=) 0.0)

      createTest testContext ((=) 0f)
      createTest testContext ((=) false)
      createTest testContext ((=) 0uy) ]
    |> testList "get segment pointers"

let expand length segmentPointers (leftMatrix: Matrix.CSR<'a>) (rightMatrix: Matrix.CSR<'b>) =
    let extendPointers pointers =
        Array.pairwise pointers
        |> Array.map (fun (fst, snd) -> snd - fst)
        |> Array.mapi (fun index length -> Array.create length index)
        |> Array.concat

    let segmentsLengths =
        Array.append segmentPointers [| length |]
        |> Array.pairwise
        |> Array.map (fun (fst, snd) -> snd - fst)

    let leftMatrixValues, expectedRows =
        let tripleFst (fst, _, _) = fst

        Array.zip3 segmentsLengths leftMatrix.Values
        <| extendPointers leftMatrix.RowPointers
        // select items each segment length not zero
        |> Array.filter (tripleFst >> ((=) 0) >> not)
        |> Array.collect (fun (length, value, rowIndex) -> Array.create length (value, rowIndex))
        |> Array.unzip

    let rightMatrixValues, expectedColumns =
        let valuesAndColumns =
            Array.zip rightMatrix.Values rightMatrix.ColumnIndices

        Array.map2
            (fun column length ->
                let rowStart = rightMatrix.RowPointers.[column]
                Array.take length valuesAndColumns.[rowStart..])
            leftMatrix.ColumnIndices
            segmentsLengths
        |> Array.concat
        |> Array.unzip

    leftMatrixValues, rightMatrixValues, expectedColumns, expectedRows

let makeExpandTest (testContext: TestContext) isEqual zero testFun (leftArray: 'a [,], rightArray: 'a [,]) =
    let context = testContext.ClContext
    let processor = testContext.Queue

    let leftMatrix =
        createCSRMatrix leftArray <| isEqual zero

    let rightMatrix =
        createCSRMatrix rightArray <| isEqual zero

    let segmentPointers, length =
        getSegmentsPointers leftMatrix rightMatrix

    let expectedLeftMatrixValues, expectedRightMatrixValues, expectedColumns, expectedRows =
        expand length segmentPointers leftMatrix rightMatrix

    if leftMatrix.NNZ > 0
       && rightMatrix.NNZ > 0
       && expectedColumns.Length > 0 then
        let clLeftMatrix = leftMatrix.ToDevice context
        let clRightMatrix = rightMatrix.ToDevice context
        let clSegmentPointers = context.CreateClArray segmentPointers

        let (clActualLeftValues: ClArray<'a>,
             clActualRightValues: ClArray<'a>,
             clActualColumns: ClArray<int>,
             clActualRows: ClArray<int>) =
            testFun processor length clSegmentPointers clLeftMatrix clRightMatrix

        clLeftMatrix.Dispose processor
        clRightMatrix.Dispose processor
        clSegmentPointers.Free processor

        let actualLeftValues =
            clActualLeftValues.ToHostAndFree processor

        let actualRightValues =
            clActualRightValues.ToHostAndFree processor

        let actualColumns = clActualColumns.ToHostAndFree processor
        let actualRows = clActualRows.ToHostAndFree processor

        "Left values must be the same"
        |> Utils.compareArrays isEqual actualLeftValues expectedLeftMatrixValues

        "Right values must be the same"
        |> Utils.compareArrays isEqual actualRightValues expectedRightMatrixValues

        "Columns must be the same"
        |> Utils.compareArrays (=) actualColumns expectedColumns

        "Rows must be the same"
        |> Utils.compareArrays (=) actualRows expectedRows

let createExpandTest (testContext: TestContext) (isEqual: 'a -> 'a -> bool) (zero: 'a) =
    let testFun =
        Expand.expand testContext.ClContext Utils.defaultWorkGroupSize

    makeExpandTest testContext isEqual zero testFun
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

// expand phase tests
let expandTests (testContext: TestContext) =
    [ createExpandTest testContext (=) 0

      if Utils.isFloat64Available testContext.ClContext.ClDevice then
          createExpandTest testContext Utils.floatIsEqual 0.0

      createExpandTest testContext Utils.float32IsEqual 0f
      createExpandTest testContext (=) false
      createExpandTest testContext (=) 0uy ]
    |> testList "Expand.expand"

let checkGeneralResult isEqual (actualMatrix: Matrix<'a>) (expectedMatrix: Matrix<'a>) =

    match actualMatrix, expectedMatrix with
    | Matrix.COO actualMatrix, Matrix.COO expected ->

        "Values must be the same"
        |> Utils.compareArrays isEqual actualMatrix.Values expected.Values

        "Columns must be the same"
        |> Utils.compareArrays (=) actualMatrix.Columns expected.Columns

        "Rows must be the same"
        |> Utils.compareArrays (=) actualMatrix.Rows expected.Rows
    | _ -> failwith "Matrix format are not matching"

let makeGeneralTest
    (testContext: TestContext)
    zero
    isEqual
    opMul
    opAdd
    testFun
    (leftArray: 'a [,], rightArray: 'a [,])
    =
    let context = testContext.ClContext
    let processor = testContext.Queue

    let leftMatrix =
        Utils.createMatrixFromArray2D CSR leftArray (isEqual zero)

    let rightMatrix =
        Utils.createMatrixFromArray2D CSR rightArray (isEqual zero)

    let matrixExpected =
        HostPrimitives.array2DMultiplication zero opMul opAdd leftArray rightArray
        |> fun array -> Utils.createMatrixFromArray2D COO array (isEqual zero)

    if leftMatrix.NNZ > 0
       && rightMatrix.NNZ > 0
       && matrixExpected.NNZ > 0 then
        let clLeftMatrix = leftMatrix.ToDevice context
        let clRightMatrix = rightMatrix.ToDevice context

        let (clMatrixActual: ClMatrix<_>) =
            testFun processor HostInterop clLeftMatrix clRightMatrix

        let matrixActual = clMatrixActual.ToHost processor

        clMatrixActual.Dispose processor

        checkGeneralResult isEqual matrixActual matrixExpected

let createGeneralTest (testContext: TestContext) (zero: 'a) isEqual (opAddQ, opAdd) (opMulQ, opMul) testFun =

    let testFun =
        testFun testContext.ClContext Utils.defaultWorkGroupSize opAddQ opMulQ

    makeGeneralTest testContext zero isEqual opMul opAdd testFun
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let generalTests (testContext: TestContext) =
    [ createGeneralTest testContext 0 (=) ArithmeticOperations.intAdd ArithmeticOperations.intMul Matrix.SpGeMM.expand

      if Utils.isFloat64Available testContext.ClContext.ClDevice then
          createGeneralTest
              testContext
              0.0
              Utils.floatIsEqual
              ArithmeticOperations.floatAdd
              ArithmeticOperations.floatMul
              Matrix.SpGeMM.expand

      createGeneralTest
          testContext
          0.0f
          Utils.float32IsEqual
          ArithmeticOperations.float32Add
          ArithmeticOperations.float32Mul
          Matrix.SpGeMM.expand

      createGeneralTest
          testContext
          false
          (=)
          ArithmeticOperations.boolAdd
          ArithmeticOperations.boolMul
          Matrix.SpGeMM.expand ]

let gpuTests =
    TestCases.gpuTests "SpGeMM tests" generalTests
