module GraphBLAS.FSharp.Tests.Matrix.SpGeMM.Expand

open Expecto
open GraphBLAS.FSharp.Backend.Matrix.SpGeMM
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

processor.Error.Add(fun e -> failwithf "%A" e)

let config =
    { Utils.defaultConfig with
          arbitrary =
              [ typeof<Generators.VectorXMatrix>
                typeof<Generators.PairOfMatricesOfCompatibleSize> ] }

let makeTest isZero testFun (leftArray: 'a [], rightArray: 'a [,]) =

    let leftMatrixRow =
        Vector.Sparse.FromArray(leftArray, isZero)

    let rightMatrix =
        Matrix.CSR.FromArray2D(rightArray, isZero)

    if leftMatrixRow.NNZ > 0 && rightMatrix.NNZ > 0 then

        // compute expected result
        let rightMatrixRowsLength =
            rightMatrix.RowPointers
            |> Array.pairwise
            |> Array.map (fun (fst, snd) -> snd - fst)

        let expectedPointers, expectedLength =
            Array.init leftMatrixRow.Indices.Length (fun index -> rightMatrixRowsLength.[leftMatrixRow.Indices.[index]])
            |> HostPrimitives.prefixSumExclude 0 (+)

        let clLeftMatrixRow = leftMatrixRow.ToDevice context

        let clRightMatrixRowsLength =
            context.CreateClArray rightMatrixRowsLength

        let actualLength, (clActual: ClArray<int>) =
            testFun processor clLeftMatrixRow clRightMatrixRowsLength

        clLeftMatrixRow.Dispose processor

        let actualPointers = clActual.ToHostAndFree processor

        "Results lengths must be the same"
        |> Expect.equal actualLength expectedLength

        "Result pointers must be the same"
        |> Expect.sequenceEqual actualPointers expectedPointers

let createTest<'a when 'a: struct> (isZero: 'a -> bool) =
    Expand.getSegmentPointers context Utils.defaultWorkGroupSize
    |> makeTest isZero
    |> testPropertyWithConfig config $"test on {typeof<'a>}"

// Debug tests
let getSegmentsTests =
    [ createTest ((=) 0)

      if Utils.isFloat64Available context.ClDevice then
          createTest ((=) 0.0)

      createTest ((=) 0f)
      createTest ((=) false)
      createTest ((=) 0uy) ]
    |> testList "get segment pointers"

let expand (leftMatrixRow: Vector.Sparse<'a>) (rightMatrix: Matrix.CSR<'b>) =
    let rightMatrixRowsLengths =
        rightMatrix.RowPointers
        |> Array.pairwise
        |> Array.map (fun (fst, snd) -> snd - fst)

    let segmentsLengths =
        Array.map (fun columnIndex -> rightMatrixRowsLengths.[columnIndex]) leftMatrixRow.Indices

    let leftMatrixValues =
        Array.map2 Array.create segmentsLengths leftMatrixRow.Values
        |> Array.concat

    let rightMatrixRowPointers =
        Array.map (fun index -> rightMatrix.RowPointers.[index]) leftMatrixRow.Indices

    let rightMatrixValues =
        Array.map2
            (fun rowPointer segmentLength -> Array.take segmentLength rightMatrix.Values.[rowPointer..])
            rightMatrixRowPointers
            segmentsLengths
        |> Array.concat

    let columns =
        Array.map2
            (fun rowPointer segmentLength -> Array.take segmentLength rightMatrix.ColumnIndices.[rowPointer..])
            rightMatrixRowPointers
            segmentsLengths
        |> Array.concat

    leftMatrixValues, rightMatrixValues, columns

let makeExpandTest isEqual zero testFun (leftArray: 'a [], rightArray: 'a [,]) =

    let leftMatrixRow =
        Vector.Sparse.FromArray(leftArray, (isEqual zero))

    let rightMatrix =
        Matrix.CSR.FromArray2D(rightArray, (isEqual zero))

    if leftMatrixRow.NNZ > 0 && rightMatrix.NNZ > 0 then

        let clPointers, lenght =
            rightMatrix.RowPointers
            |> Array.pairwise
            |> Array.map (fun (fst, snd) -> snd - fst)
            |> fun rightMatrixRowsLengths ->
                Array.init
                    leftMatrixRow.Indices.Length
                    (fun index -> rightMatrixRowsLengths.[leftMatrixRow.Indices.[index]])
            |> HostPrimitives.prefixSumExclude 0 (+)
            |> fun (pointers, length) -> context.CreateClArray(pointers), length

        let clLeftMatrixRow = leftMatrixRow.ToDevice context
        let clRightMatrix = rightMatrix.ToDevice context

        let result =
            testFun processor lenght clPointers clLeftMatrixRow clRightMatrix

        clLeftMatrixRow.Dispose processor
        clRightMatrix.Dispose processor
        clPointers.Free processor

        let expectedLeftMatrixValues, expectedRightMatrixValues, expectedColumns = expand leftMatrixRow rightMatrix

        match result with
        | Some (clActualLeftValues: ClArray<'a>, clActualRightValues: ClArray<'a>, clActualColumns: ClArray<int>) ->

            let actualLeftValues =
                clActualLeftValues.ToHostAndFree processor

            let actualRightValues =
                clActualRightValues.ToHostAndFree processor

            let actualColumns = clActualColumns.ToHostAndFree processor

            "Left values must be the same"
            |> Utils.compareArrays isEqual actualLeftValues expectedLeftMatrixValues

            "Right values must be the same"
            |> Utils.compareArrays isEqual actualRightValues expectedRightMatrixValues

            "Columns must be the same"
            |> Utils.compareArrays (=) actualColumns expectedColumns
        | None ->
            "Result must be empty"
            |> Expect.isTrue (expectedColumns.Length = 0)

let createExpandTest isEqual (zero: 'a) testFun =
    testFun context Utils.defaultWorkGroupSize
    |> makeExpandTest isEqual zero
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

// (Debug only) expand phase tests
let expandTests =
    [ createExpandTest (=) 0 Expand.expand

      if Utils.isFloat64Available context.ClDevice then
          createExpandTest Utils.floatIsEqual 0.0 Expand.expand

      createExpandTest Utils.float32IsEqual 0f Expand.expand
      createExpandTest (=) false Expand.expand
      createExpandTest (=) 0uy Expand.expand ]
    |> testList "Expand.expand"

let makeGeneralTest<'a when 'a: struct> zero isEqual opMul opAdd testFun (leftArray: 'a [,], rightArray: 'a [,]) =

    let leftMatrix =
        Utils.createMatrixFromArray2D CSR leftArray (isEqual zero)

    let rightMatrix =
        Utils.createMatrixFromArray2D CSR rightArray (isEqual zero)

    if leftMatrix.NNZ > 0 && rightMatrix.NNZ > 0 then

        let clLeftMatrix = leftMatrix.ToDevice context
        let clRightMatrix = rightMatrix.ToDevice context

        let (clMatrixActual: ClMatrix<_>) =
            testFun processor HostInterop clLeftMatrix clRightMatrix

        clLeftMatrix.Dispose processor
        clRightMatrix.Dispose processor

        let matrixActual =
            clMatrixActual.ToHostAndDispose processor

        match matrixActual with
        | Matrix.LIL actual ->
            HostPrimitives.array2DMultiplication zero opMul opAdd leftArray rightArray
            |> fun array -> Matrix.LIL.FromArray2D(array, (isEqual zero))
            |> Utils.compareLILMatrix isEqual actual
        | _ -> failwith "Matrix format are not matching"

let createGeneralTest (zero: 'a) isEqual (opAddQ, opAdd) (opMulQ, opMul) testFun =
    testFun opAddQ opMulQ context Utils.defaultWorkGroupSize
    |> makeGeneralTest<'a> zero isEqual opMul opAdd
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
