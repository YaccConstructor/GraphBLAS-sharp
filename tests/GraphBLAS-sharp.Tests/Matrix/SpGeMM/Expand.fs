module GraphBLAS.FSharp.Tests.Matrix.SpGeMM.Expand

open Expecto
open GraphBLAS.FSharp.Backend.Matrix.CSR.SpGeMM
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Test
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

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

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

let makeTest isZero testFun (leftArray: 'a [,], rightArray: 'a [,]) =

    let leftMatrix = createCSRMatrix leftArray isZero

    let rightMatrix = createCSRMatrix rightArray isZero

    if leftMatrix.NNZ > 0 && rightMatrix.NNZ > 0 then

        let clLeftMatrix = leftMatrix.ToDevice context

        let clRightMatrix = rightMatrix.ToDevice context

        let actualLength, (clActual: ClArray<int>) =
            testFun processor clLeftMatrix clRightMatrix

        clLeftMatrix.Dispose processor
        clRightMatrix.Dispose processor

        let actualPointers = clActual.ToHostAndFree processor

        let expectedPointers, expectedLength =
            getSegmentsPointers leftMatrix rightMatrix

        "Results lengths must be the same"
        |> Expect.equal actualLength expectedLength

        "Result pointers must be the same"
        |> Expect.sequenceEqual actualPointers expectedPointers

let createTest<'a when 'a: struct> (isZero: 'a -> bool) testFun =

    let testFun =
        testFun context Utils.defaultWorkGroupSize

    makeTest isZero testFun
    |> testPropertyWithConfig config $"test on {typeof<'a>}"

let getSegmentsTests =
    [ createTest ((=) 0) Expand.getSegmentPointers

      if Utils.isFloat64Available context.ClDevice then
          createTest ((=) 0.0) Expand.getSegmentPointers

      createTest ((=) 0f) Expand.getSegmentPointers
      createTest ((=) false) Expand.getSegmentPointers
      createTest ((=) 0uy) Expand.getSegmentPointers ]
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

let makeExpandTest isEqual zero testFun (leftArray: 'a [,], rightArray: 'a [,]) =

    let leftMatrix =
        createCSRMatrix leftArray <| isEqual zero

    let rightMatrix =
        createCSRMatrix rightArray <| isEqual zero

    if leftMatrix.NNZ > 0 && rightMatrix.NNZ > 0 then

        let segmentPointers, length =
            getSegmentsPointers leftMatrix rightMatrix

        let clLeftMatrix = leftMatrix.ToDevice context
        let clRightMatrix = rightMatrix.ToDevice context
        let clSegmentPointers = context.CreateClArray segmentPointers

        let ((clActualLeftValues: ClArray<'a>),
             (clActualRightValues: ClArray<'a>),
             (clActualColumns: ClArray<int>),
             (clActualRows: ClArray<int>)) =
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

        let expectedLeftMatrixValues, expectedRightMatrixValues, expectedColumns, expectedRows =
            expand length segmentPointers leftMatrix rightMatrix

        "Left values must be the same"
        |> Utils.compareArrays isEqual actualLeftValues expectedLeftMatrixValues

        "Right values must be the same"
        |> Utils.compareArrays isEqual actualRightValues expectedRightMatrixValues

        "Columns must be the same"
        |> Utils.compareArrays (=) actualColumns expectedColumns

        "Rows must be the same"
        |> Utils.compareArrays (=) actualRows expectedRows

let createExpandTest isEqual (zero: 'a) testFun =

    let testFun =
        testFun context Utils.defaultWorkGroupSize

    makeExpandTest isEqual zero testFun
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

// expand phase tests
let expandTests =
    [ createExpandTest (=) 0 Expand.expand

      if Utils.isFloat64Available context.ClDevice then
          createExpandTest Utils.floatIsEqual 0.0 Expand.expand

      createExpandTest Utils.float32IsEqual 0f Expand.expand
      createExpandTest (=) false Expand.expand
      createExpandTest (=) 0uy Expand.expand ]
    |> testList "Expand.expand"

let checkGeneralResult zero isEqual (actualMatrix: Matrix<'a>) mul add (leftArray: 'a [,]) (rightArray: 'a [,]) =

    let expected =
        HostPrimitives.array2DMultiplication zero mul add leftArray rightArray
        |> fun array -> Utils.createMatrixFromArray2D COO array (isEqual zero)

    match actualMatrix, expected with
    | Matrix.COO actualMatrix, Matrix.COO expected ->

        "Values must be the same"
        |> Utils.compareArrays isEqual actualMatrix.Values expected.Values

        "Columns must be the same"
        |> Utils.compareArrays (=) actualMatrix.Columns expected.Columns

        "Rows must be the same"
        |> Utils.compareArrays (=) actualMatrix.Rows expected.Rows
    | _ -> failwith "Matrix format are not matching"

let makeGeneralTest zero isEqual opMul opAdd testFun (leftArray: 'a [,], rightArray: 'a [,]) =

    let leftMatrix =
        Utils.createMatrixFromArray2D CSR leftArray (isEqual zero)

    let rightMatrix =
        Utils.createMatrixFromArray2D CSR rightArray (isEqual zero)

    if leftMatrix.NNZ > 0 && rightMatrix.NNZ > 0 then
        try
            let clLeftMatrix = leftMatrix.ToDevice context
            let clRightMatrix = rightMatrix.ToDevice context

            let (clMatrixActual: ClMatrix<_>) =
                testFun processor HostInterop clLeftMatrix clRightMatrix

            let matrixActual = clMatrixActual.ToHost processor
            clMatrixActual.Dispose processor

            checkGeneralResult zero isEqual matrixActual opMul opAdd leftArray rightArray
        with
        | ex when ex.Message = "InvalidBufferSize" -> ()
        | _ -> reraise ()

let createGeneralTest (zero: 'a) isEqual (opAddQ, opAdd) (opMulQ, opMul) testFun =

    let testFun =
        testFun context Utils.defaultWorkGroupSize opAddQ opMulQ

    makeGeneralTest zero isEqual opMul opAdd testFun
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let generalTests =
    [ createGeneralTest 0 (=) ArithmeticOperations.intAdd ArithmeticOperations.intMul Matrix.SpGeMM.expand

      if Utils.isFloat64Available context.ClDevice then
          createGeneralTest
              0.0
              Utils.floatIsEqual
              ArithmeticOperations.floatAdd
              ArithmeticOperations.floatMul
              Matrix.SpGeMM.expand

      createGeneralTest
          0.0f
          Utils.float32IsEqual
          ArithmeticOperations.float32Add
          ArithmeticOperations.float32Mul
          Matrix.SpGeMM.expand
      createGeneralTest false (=) ArithmeticOperations.boolAdd ArithmeticOperations.boolMul Matrix.SpGeMM.expand ]
    |> testList "general"
