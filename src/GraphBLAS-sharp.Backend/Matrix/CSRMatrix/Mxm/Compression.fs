namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Predefined

module internal Compression =
    let run
        (plus: Expr<'a -> 'a -> 'a>)
        (clContext: ClContext)
        workGroupSize =

        let decrement = ClArray.map <@ fun a -> a - 1 @> clContext workGroupSize
        let setHeadFlags = ClArray.setHeadFlags clContext workGroupSize
        let scanByHeadFlagsInclude = PrefixSum.byHeadFlagsInclude plus clContext workGroupSize
        let scanIncludeInPlace = PrefixSum.standardIncludeInplace clContext workGroupSize
        let scatterArray = Scatter.arrayInPlace clContext workGroupSize
        let scatterConst = Scatter.constInPlace 1 clContext workGroupSize
        let scatterData = Scatter.arrayInPlace clContext workGroupSize
        let scatterRowPointers = ScatterRowPointers.runInPlace clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (matrix: CSRMatrix<'a>)
            (heads: ClArray<int>)
            (zero: 'a) ->
                setHeadFlags processor matrix.Columns heads

                let scannedValues = scanByHeadFlagsInclude processor heads matrix.Values zero

                let resultLengthGpu = clContext.CreateClArray(1)
                scanIncludeInPlace processor heads resultLengthGpu
                |> ignore
                let positions = decrement processor heads
                processor.Post(Msg.CreateFreeMsg<_>(heads))

                // let resultLength =
                //     ClCell.toHost resultLengthGpu
                //     |> ClTask.runSync clContext
                let resultLength = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg(resultLengthGpu, [|0|], ch)).[0]
                processor.Post(Msg.CreateFreeMsg<_>(resultLengthGpu))

                let resultColumns =
                    clContext.CreateClArray(
                        resultLength,
                        hostAccessMode = HostAccessMode.NotAccessible
                    )

                let resultValues =
                    clContext.CreateClArray(
                        resultLength,
                        hostAccessMode = HostAccessMode.NotAccessible
                    )

                scatterArray processor positions matrix.Columns resultColumns
                scatterData processor positions scannedValues resultValues
                processor.Post(Msg.CreateFreeMsg<_>(scannedValues))
                scatterRowPointers processor positions matrix.Columns.Length resultLength matrix.RowPointers
                processor.Post(Msg.CreateFreeMsg<_>(positions))

                {
                    Context = clContext
                    RowCount = matrix.RowCount
                    ColumnCount = matrix.ColumnCount
                    RowPointers = matrix.RowPointers
                    Columns = resultColumns
                    Values = resultValues
                }
