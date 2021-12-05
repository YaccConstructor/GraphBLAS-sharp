namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Predefined

module internal rec Expansion =
    let run
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>)
        (matrixRight: CSRMatrix<'a>)
        (resultMatrix: CSRMatrix<'a>)
        (times: Expr<'a -> 'a -> 'a>) =

        let headFlags = getHeadFlags clContext workGroupSize processor matrixLeft

        let p2 = getP2 clContext workGroupSize processor headFlags matrixLeft matrixRight
        let p2_ = getP2' clContext workGroupSize processor headFlags matrixLeft matrixRight resultMatrix

        let resultHeadFlags = getResultHeadFlags clContext workGroupSize processor p2_ resultMatrix

        let secondPositions = getSecondPositions clContext workGroupSize processor p2 p2_ resultHeadFlags resultMatrix
        let firstPositions = getFirstPositions clContext workGroupSize processor p2_ resultMatrix

        let resultColumns = Gather.run clContext workGroupSize processor secondPositions matrixRight.Columns
        let firstValues = Gather.run clContext workGroupSize processor firstPositions matrixLeft.Values
        let secondValues = Gather.run clContext workGroupSize processor secondPositions matrixRight.Values

        let resultValues = ClArray.zipWith clContext workGroupSize processor times firstValues secondValues

        {
            RowCount = resultMatrix.RowCount
            ColumnCount = resultMatrix.ColumnCount
            RowPointers = resultMatrix.RowPointers
            Columns = resultColumns
            Values = resultValues
        }

    let private getFirstPositions
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (p2_: ClArray<int>)
        (resultMatrix: CSRMatrix<'a>) =

        let positions = ClArray.create clContext workGroupSize processor resultMatrix.Columns.Length 0

        updateFirstPositions clContext workGroupSize processor p2_ positions

        let max = <@ fun a b -> if a > b then a else b @>
        let total = clContext.CreateClArray(1)
        PrefixSum.runIncludeInplace clContext workGroupSize processor positions total max 0
        |> ignore
        processor.Post(Msg.CreateFreeMsg(total))

        positions
        // ByHeadFlags.runInclude clContext workGroupSize processor headFlags positions max 0

    let private updateFirstPositions
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (p2_: ClArray<int>)
        (positions: ClArray<int>) =

        let size = p2_.Length

        let update =
            <@
                fun (ndRange: Range1D)
                    (p2_: ClArray<int>)
                    (positions: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        positions.[p2_.[i]] <- i
            @>

        let kernel = clContext.CreateClKernel(update)
        let ndRange = Range1D.CreateValid(size, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        p2_
                        positions)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private getSecondPositions
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (p2: ClArray<int>)
        (p2_: ClArray<int>)
        (headFlags: ClArray<int>)
        (resultMatrix: CSRMatrix<'a>) =

        let positions = ClArray.create clContext workGroupSize processor resultMatrix.Columns.Length 1
        updateSecondPositions clContext workGroupSize processor p2 p2_ positions
        PrefixSum.byHeadFlagsInclude clContext workGroupSize processor headFlags positions <@ (+) @> 0

    let private getResultHeadFlags
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (p2_: ClArray<int>)
        (resultMatrix: CSRMatrix<'a>) =

        let headFlags = ClArray.create clContext workGroupSize processor resultMatrix.Columns.Length 0

        let size = resultMatrix.RowCount

        let init =
            <@
                fun (ndRange: Range1D)
                    (p2_: ClArray<int>)
                    (heads: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        heads.[p2_.[i]] <- 1
            @>

        let kernel = clContext.CreateClKernel(init)
        let ndRange = Range1D.CreateValid(size, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        p2_
                        headFlags)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

        headFlags

    let private updateSecondPositions
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (p2: ClArray<int>)
        (p2_: ClArray<int>)
        (positions: ClArray<int>) =

        let size = p2_.Length

        let update =
            <@
                fun (ndRange: Range1D)
                    (p2: ClArray<int>)
                    (p2_: ClArray<int>)
                    (positions: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        positions.[p2_.[i]] <- p2.[i]
            @>

        let kernel = clContext.CreateClKernel(update)
        let ndRange = Range1D.CreateValid(size, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        p2
                        p2_
                        positions)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private getHeadFlags
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>) =

        let headFlags = ClArray.create clContext workGroupSize processor matrixLeft.Columns.Length 0

        let size = matrixLeft.RowCount

        let init =
            <@
                fun (ndRange: Range1D)
                    (rowPointers: ClArray<int>)
                    (heads: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        heads.[rowPointers.[i]] <- 1
            @>

        let kernel = clContext.CreateClKernel(init)
        let ndRange = Range1D.CreateValid(size, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        matrixLeft.RowPointers
                        headFlags)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

        headFlags

    let private getP2
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (headFlags: ClArray<int>)
        (matrixLeft: CSRMatrix<'a>)
        (matrixRight: CSRMatrix<'a>) =

        let p2 = initP2 clContext workGroupSize processor matrixLeft matrixRight
        updateP2 clContext workGroupSize processor matrixLeft matrixRight p2
        PrefixSum.byHeadFlagsInclude clContext workGroupSize processor headFlags p2 <@ (+) @> 0

    let private getP2'
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (headFlags: ClArray<int>)
        (matrixLeft: CSRMatrix<'a>)
        (matrixRight: CSRMatrix<'a>)
        (resultMatrix: CSRMatrix<'a>) =

        let p2_ = initP2' clContext workGroupSize processor matrixLeft matrixRight
        updateP2' clContext workGroupSize processor matrixLeft resultMatrix p2_
        PrefixSum.byHeadFlagsInclude clContext workGroupSize processor headFlags p2_ <@ (+) @> 0

    let private initP2'
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>)
        (matrixRight: CSRMatrix<'a>) =

        let p2_ =
            clContext.CreateClArray(
                matrixLeft.Columns.Length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        let size = p2_.Length

        let init =
            <@
                fun (ndRange: Range1D)
                    (secondRowPointers: ClArray<int>)
                    (firstColumns: ClArray<int>)
                    (p2_: ClArray<int>) ->

                    let i = ndRange.GlobalID0 + 1

                    if i < size then
                        p2_.[i] <- secondRowPointers.[firstColumns.[i - 1] + 1] - secondRowPointers.[firstColumns.[i - 1]]
            @>

        let kernel = clContext.CreateClKernel(init)
        let ndRange = Range1D.CreateValid(size - 1, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        matrixRight.RowPointers
                        matrixLeft.Columns
                        p2_)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))
        p2_

    let private updateP2'
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>)
        (resultMatrix: CSRMatrix<'a>)
        (p2_: ClArray<int>) =

        let size = matrixLeft.RowCount

        let update =
            <@
                fun (ndRange: Range1D)
                    (firstRowPointers: ClArray<int>)
                    (resultRowPointers: ClArray<int>)
                    (p2_: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        let index = firstRowPointers.[i]
                        if index <> firstRowPointers.[i + 1] then
                            p2_.[index] <- resultRowPointers.[i]
            @>

        let kernel = clContext.CreateClKernel(update)
        let ndRange = Range1D.CreateValid(size - 1, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        matrixLeft.RowPointers
                        resultMatrix.RowPointers
                        p2_)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private initP2
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>)
        (matrixRight: CSRMatrix<'a>) =

        let p2 =
            clContext.CreateClArray(
                matrixLeft.Columns.Length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        let size = p2.Length

        let init =
            <@
                fun (ndRange: Range1D)
                    (secondRowPointers: ClArray<int>)
                    (firstColumns: ClArray<int>)
                    (p2: ClArray<int>) ->

                    let i = ndRange.GlobalID0 + 1

                    if i < size then
                        p2.[i] <- secondRowPointers.[firstColumns.[i]] - secondRowPointers.[firstColumns.[i - 1]]
            @>

        let kernel = clContext.CreateClKernel(init)
        let ndRange = Range1D.CreateValid(size - 1, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        matrixRight.RowPointers
                        matrixLeft.Columns
                        p2)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

        p2

    let private updateP2
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>)
        (matrixRight: CSRMatrix<'a>)
        (p2: ClArray<int>) =

        let size = matrixLeft.RowCount

        let update =
            <@
                fun (ndRange: Range1D)
                    (firstRowPointers: ClArray<int>)
                    (firstColumns: ClArray<int>)
                    (secondRowPointers: ClArray<int>)
                    (p2: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        let index = firstRowPointers.[i]
                        if index <> firstRowPointers.[i + 1] then
                            p2.[index] <- secondRowPointers.[firstColumns.[index]]
            @>

        let kernel = clContext.CreateClKernel(update)
        let ndRange = Range1D.CreateValid(size - 1, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        matrixLeft.RowPointers
                        matrixLeft.Columns
                        matrixRight.RowPointers
                        p2)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))
