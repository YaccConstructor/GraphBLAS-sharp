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

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let config = { Utils.defaultConfig with arbitrary = [ typeof<Generators.PairOfMatricesOfCompatibleSize> ] }

let getSegmentsPointers (leftMatrix: Matrix.CSR<'a>) (rightMatrix: Matrix.CSR<'b>) =
    Array.map (fun item ->
        rightMatrix.RowPointers.[item + 1] - rightMatrix.RowPointers.[item]) leftMatrix.ColumnIndices
    |> HostPrimitives.prefixSumExclude

let makeTest isZero testFun (leftArray: 'a [,], rightArray: 'a [,]) =

    let leftMatrix =
        Utils.createMatrixFromArray2D CSR leftArray isZero
        |> Utils.castMatrixToCSR

    let rightMatrix =
        Utils.createMatrixFromArray2D CSR rightArray isZero
        |> Utils.castMatrixToCSR

    if leftMatrix.NNZ > 0 && rightMatrix.NNZ > 0 then

        let clLeftMatrix = leftMatrix.ToDevice context

        let clRightMatrix = rightMatrix.ToDevice context

        let actualLength, (clActual: ClArray<int>) =
            testFun processor clLeftMatrix clRightMatrix

        clLeftMatrix.Dispose processor

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
    |> testPropertyWithConfig  { config with endSize = 10 } $"test on {typeof<'a>}"

let getSegmentsTests =
    [ createTest ((=) 0) Expand.getSegmentPointers

      if Utils.isFloat64Available context.ClDevice then
        createTest ((=) 0.0) Expand.getSegmentPointers

      createTest ((=) 0f) Expand.getSegmentPointers
      createTest ((=) false) Expand.getSegmentPointers
      createTest ((=) 0u) Expand.getSegmentPointers ]
    |> testList "get segment pointers"

let makeExpandTest isZero testFun (leftArray: 'a [,], rightArray: 'a [,]) =

        let leftMatrix =
            Utils.createMatrixFromArray2D CSR leftArray isZero
            |> Utils.castMatrixToCSR

        let rightMatrix =
            Utils.createMatrixFromArray2D CSR rightArray isZero
            |> Utils.castMatrixToCSR

        if leftMatrix.NNZ > 0
           && rightMatrix.NNZ > 0 then

            let segmentPointers, length =
                getSegmentsPointers leftMatrix rightMatrix

            let clLeftMatrix = leftMatrix.ToDevice context
            let clRightMatrix = rightMatrix.ToDevice context
            let clSegmentPointers = context.CreateClArray segmentPointers

            let (actualValues: ClArray<'a>), (actualColumns: ClArray<int>), (actualRows: ClArray<int>) =
                testFun processor length clSegmentPointers clLeftMatrix clRightMatrix

            clLeftMatrix.Free processor
            clRightMatrix. processor
            clSegmentPointers

