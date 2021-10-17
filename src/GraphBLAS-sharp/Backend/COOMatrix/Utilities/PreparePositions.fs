namespace GraphBLAS.FSharp.Backend.COOMatrix.Utilities

open Brahma.FSharp.OpenCL
open OpenCL.Net
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common

[<AutoOpen>]
module internal PreparePositions =
    let preparePositions<'a when 'a: struct> (clContext: ClContext) (opAdd: Expr<'a -> 'a -> 'a>) workGroupSize =

        let preparePositions =
            <@ fun (ndRange: Range1D) length (allRowsBuffer: ClArray<int>) (allColumnsBuffer: ClArray<int>) (allValuesBuffer: ClArray<'a>) (rawPositionsBuffer: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if (i < length - 1
                    && allRowsBuffer.[i] = allRowsBuffer.[i + 1]
                    && allColumnsBuffer.[i] = allColumnsBuffer.[i + 1]) then
                    rawPositionsBuffer.[i] <- 0
                    allValuesBuffer.[i + 1] <- (%opAdd) allValuesBuffer.[i] allValuesBuffer.[i + 1]
                else
                    (rawPositionsBuffer.[i] <- 1) @>

        let kernel =
            clContext.CreateClKernel(preparePositions)

        fun (processor: MailboxProcessor<_>) (allRows: ClArray<int>) (allColumns: ClArray<int>) (allValues: ClArray<'a>) ->
            let length = allValues.Length

            let ndRange =
                Range1D(Utils.getDefaultGlobalSize (length - 1), workGroupSize)

            let rawPositionsGpu =
                clContext.CreateClArray<int>(length, hostAccessMode = HostAccessMode.NotAccessible)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.SetArguments ndRange length allRows allColumns allValues rawPositionsGpu)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))
            rawPositionsGpu
