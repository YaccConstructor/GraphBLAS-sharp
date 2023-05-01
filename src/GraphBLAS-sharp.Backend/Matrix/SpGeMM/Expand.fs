namespace GraphBLAS.FSharp.Backend.Matrix.SpGeMM

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Common.Sort
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClCell
open FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects.ClVector
open GraphBLAS.FSharp.Backend.Objects.ClMatrix

module Expand =
    let getSegmentPointers (clContext: ClContext) workGroupSize =

        let gather = Gather.run clContext workGroupSize

        let prefixSum =
            PrefixSum.standardExcludeInPlace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (leftMatrixRow: ClMatrix.CSR<'a>) (rightMatrixRowsLengths: ClArray<int>) ->

            let segmentsLengths =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, leftMatrixRow.NNZ)

            // extract needed lengths by left matrix nnz
            gather processor leftMatrixRow.Columns rightMatrixRowsLengths segmentsLengths

            // compute pointers
            let length =
                (prefixSum processor segmentsLengths)
                    .ToHostAndFree processor

            length, segmentsLengths

    let runByRows (clContext: ClContext) workGroupSize =

        fun (processor: MailboxProcessor<_>) startRow endRow (leftMatrix: ClMatrix.CSR<'a>) (rightMatrix: ClMatrix.CSR<'a>) ->



            ()

    let CUSP (clContext: ClContext) workGroupSize =

        let getNNZInRows =
            CSR.Matrix.NNZInRows clContext workGroupSize

        let getSegmentPointers =
            getSegmentPointers clContext workGroupSize

        let gather =
            Gather.run clContext workGroupSize

        fun (processor: MailboxProcessor<_>) maxAllocSize (leftMatrix: ClMatrix.CSR<'a>) (rightMatrix: ClMatrix.CSR<'b>) ->

            let rightMatrixRowsNNZ =
                getNNZInRows processor DeviceOnly rightMatrix

            let length, segmentLengths =
                getSegmentPointers processor leftMatrix rightMatrixRowsNNZ

            if length < maxAllocSize then
                // compute in one step

                ()
            else
                // extract segment lengths by left matrix rows pointers
                let segmentPointersByLeftMatrixRows =
                    clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, leftMatrix.RowPointers.Length)

                gather processor segmentLengths leftMatrix.RowPointers segmentPointersByLeftMatrixRows

                let segmentPointersByLeftMatrixRows = segmentPointersByLeftMatrixRows.ToHostAndFree processor

                let beginRow = 0
                let totalWork = 0

                while beginRow < leftMatrix.RowCount do

                    //let endRow =

                ()

            ()
