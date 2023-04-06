module GraphBLAS.FSharp.Tests.Matrix.SpGeMM

open Expecto
open GraphBLAS.FSharp.Backend.Matrix.CSR.SpGeMM
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

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let config = { Utils.defaultConfig with arbitrary = [ typeof<Generators.PairOfMatricesOfCompatibleSize> ] }

let createCSRMatrix array isZero =
    Utils.createMatrixFromArray2D CSR array isZero
    |> Utils.castMatrixToCSR

let getSegmentsPointers (leftMatrix: Matrix.CSR<'a>) (rightMatrix: Matrix.CSR<'b>) =
    Array.map (fun item ->
        rightMatrix.RowPointers.[item + 1] - rightMatrix.RowPointers.[item]) leftMatrix.ColumnIndices
    |> HostPrimitives.prefixSumExclude

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

let createTest<'a when 'a : struct> (isZero: 'a -> bool) testFun =

    let testFun = testFun context Utils.defaultWorkGroupSize

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

let expand length segmentPointers mulOp (leftMatrix: Matrix.CSR<'a>) (rightMatrix: Matrix.CSR<'b>) =
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

        Array.zip3 segmentsLengths leftMatrix.Values <| extendPointers leftMatrix.RowPointers // TODO(expand row pointers)
        // select items each segment length not zero
        |> Array.filter (tripleFst >> ((=) 0) >> not)
        |> Array.collect (fun (length, value, rowIndex) -> Array.create length (value, rowIndex))
        |> Array.unzip

    let rightMatrixValues, expectedColumns =
        let valuesAndColumns = Array.zip rightMatrix.Values rightMatrix.ColumnIndices

        Array.map2 (fun column length ->
            let rowStart = rightMatrix.RowPointers.[column]
            Array.take length valuesAndColumns.[rowStart..]) leftMatrix.ColumnIndices segmentsLengths
        |> Array.concat
        |> Array.unzip

    let expectedValues = Array.map2 mulOp leftMatrixValues rightMatrixValues

    expectedValues, expectedColumns, expectedRows

let makeExpandTest isEqual zero opMul testFun (leftArray: 'a [,], rightArray: 'a [,]) =

        let leftMatrix = createCSRMatrix leftArray <| isEqual zero

        let rightMatrix = createCSRMatrix rightArray <| isEqual zero

        if leftMatrix.NNZ > 0
           && rightMatrix.NNZ > 0 then

            let segmentPointers, length =
                getSegmentsPointers leftMatrix rightMatrix

            let clLeftMatrix = leftMatrix.ToDevice context
            let clRightMatrix = rightMatrix.ToDevice context
            let clSegmentPointers = context.CreateClArray segmentPointers

            let (clActualValues: ClArray<'a>), (clActualColumns: ClArray<int>), (clActualRows: ClArray<int>) =
                testFun processor length clSegmentPointers clLeftMatrix clRightMatrix

            clLeftMatrix.Dispose processor
            clRightMatrix.Dispose processor
            clSegmentPointers.Free processor

            let actualValues = clActualValues.ToHostAndFree processor
            let actualColumns = clActualColumns.ToHostAndFree processor
            let actualRows = clActualRows.ToHostAndFree processor

            let expectedValues, expectedColumns, expectedRows =
                expand length segmentPointers opMul leftMatrix rightMatrix

            "Values must be the same"
            |> Utils.compareArrays isEqual actualValues expectedValues

            "Columns must be the same"
            |> Utils.compareArrays (=) actualColumns expectedColumns

            "Rows must be the same"
            |> Utils.compareArrays (=) actualRows expectedRows

let createExpandTest isEqual (zero: 'a) opMul opMulQ testFun =

    let testFun = testFun context Utils.defaultWorkGroupSize opMulQ

    makeExpandTest isEqual zero opMul testFun
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let expandTests =
    [ createExpandTest (=) 0 (*) <@ (*) @> Expand.expand

      if Utils.isFloat64Available context.ClDevice then
        createExpandTest Utils.floatIsEqual 0.0 (*) <@ (*) @> Expand.expand

      createExpandTest Utils.float32IsEqual 0f (*) <@ (*) @> Expand.expand
      createExpandTest (=) false (&&) <@ (&&) @> Expand.expand
      createExpandTest (=) 0uy (*) <@ (*) @> Expand.expand ]
    |> testList "Expand.expand"

let checkGeneralResult zero isEqual actualValues actualColumns actualRows mul add (leftArray: 'a [,]) (rightArray: 'a [,]) =

    let expected =
        HostPrimitives.array2DMultiplication mul add leftArray rightArray
        |> fun array -> Utils.createMatrixFromArray2D COO array (isEqual zero)
        |> function Matrix.COO matrix -> matrix | _ -> failwith "format miss"

    printfn $"leftMatrix \n %A{leftArray}"
    printfn $"rightMatrix \n %A{rightArray}"

    printfn $"actual values: %A{actualValues}"
    printfn $"expected values: %A{expected.Values}"

    printfn $"actualColumns: %A{actualColumns}"
    printfn $"expectedColumns: %A{expected.Columns}"

    printfn $"actualRows: %A{actualRows}"
    printfn $"expectedRows: %A{expected.Rows}"

    "Values must be the same"
    |> Utils.compareArrays isEqual actualValues expected.Values

    "Columns must be the same"
    |> Utils.compareArrays (=) actualColumns expected.Columns

    "Rows must be the same"
    |> Utils.compareArrays (=) actualRows expected.Rows

let makeGeneralTest zero isEqual opMul opAdd testFun (leftArray: 'a [,], rightArray: 'a [,]) =

        let leftMatrix = createCSRMatrix leftArray <| isEqual zero

        let rightMatrix = createCSRMatrix rightArray <| isEqual zero

        if leftMatrix.NNZ > 0
           && rightMatrix.NNZ > 0 then
           try
                let clLeftMatrix = leftMatrix.ToDevice context
                let clRightMatrix = rightMatrix.ToDevice context

                let (clActualValues: ClArray<'a>), (clActualColumns: ClArray<int>), (clActualRows: ClArray<int>) =
                    testFun processor HostInterop clLeftMatrix clRightMatrix

                let actualValues = clActualValues.ToHostAndFree processor
                let actualColumns = clActualColumns.ToHostAndFree processor
                let actualRows = clActualRows.ToHostAndFree processor

                checkGeneralResult zero isEqual actualValues actualColumns actualRows opMul opAdd leftArray rightArray
           with
           | ex when ex.Message = "InvalidBufferSize" -> ()
           | ex -> raise ex

let createGeneralTest (zero: 'a) isEqual opAdd opAddQ opMul opMulQ testFun =

    let testFun = testFun context Utils.defaultWorkGroupSize opAddQ opMulQ

    makeGeneralTest zero isEqual opMul opAdd testFun
    |> testPropertyWithConfig { config with endSize = 10; maxTest = 1000 } $"test on %A{typeof<'a>}"

let generalTests =
    [ createGeneralTest 0 (=) (+) <@ (+) @> (*) <@ (*) @> Expand.run ]
    |> testList "general"
