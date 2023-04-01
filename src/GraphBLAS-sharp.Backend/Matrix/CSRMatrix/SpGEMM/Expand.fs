namespace GraphBLAS.FSharp.Backend.Matrix.CSR.SpGEMM

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClCell

type Indices = ClArray<int>

type Values<'a> = ClArray<'a>

module Expand =

    let getRowsLengths (clContext: ClContext) workGroupSize =

        let create =
            ClArray.init clContext workGroupSize Map.inc

        let zeroCreate = ClArray.zeroCreate clContext workGroupSize

        let scatter = Scatter.runInplace clContext workGroupSize

        let subtraction = ClArray.map2 clContext workGroupSize Map.subtraction

        fun (processor: MailboxProcessor<_>) allocationMode (rowPointers: Indices) ->

            let positions = create processor DeviceOnly rowPointers.Length

            let shiftedPointers = zeroCreate processor DeviceOnly rowPointers.Length

            scatter processor positions rowPointers shiftedPointers

            let result =
                subtraction processor allocationMode shiftedPointers rowPointers

            positions.Free processor
            rowPointers.Free processor

            result

    let expand (clContext: ClContext) workGroupSize =
        let init = ClArray.init clContext workGroupSize Map.id

        let scatter = Scatter.runInplace clContext workGroupSize

        let zeroCreate = ClArray.zeroCreate clContext workGroupSize

        let maxPrefixSum = PrefixSum.runIncludeInplace <@ max @> clContext workGroupSize

        let initWithUnits = ClArray.init clContext workGroupSize <@ fun _ -> 1 @>

        fun (processor: MailboxProcessor<_>) lengths (segmentLengths: Indices) (leftMatrix: ClMatrix.CSR<'a>) (rightMatrix: ClMatrix.CSR<'b>) ->

            // Compute A positions
            let sequence = init processor DeviceOnly segmentLengths.Length

            let APositions = zeroCreate processor DeviceOnly lengths

            scatter processor segmentLengths sequence APositions

            sequence.Free processor

            maxPrefixSum processor APositions 0

            // Compute B positions





    let run (clContext: ClContext) workGroupSize =

        let getRowsLengths = getRowsLengths clContext workGroupSize

        let zeroCreate = ClArray.zeroCreate clContext workGroupSize

        let prefixSum = PrefixSum.standardExcludeInplace clContext workGroupSize

        let gather = Gather.run clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (leftMatrix: ClMatrix.CSR<'a>) (rightMatrix: ClMatrix.CSR<'b>) ->

            let bRowsLengths = getRowsLengths processor DeviceOnly rightMatrix.RowPointers

            let segmentsLengths = zeroCreate processor DeviceOnly leftMatrix.Columns.Length
            gather processor leftMatrix.Columns bRowsLengths segmentsLengths

            bRowsLengths.Free processor

            let length = (prefixSum processor segmentsLengths).ToHostAndFree processor



            ()

