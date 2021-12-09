namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Predefined

module internal Expansion =
    let private updateSecondPositions
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (rowPtrsForCols: ClArray<int>)
        (positionsForResCols: ClArray<int>)
        (positions: ClArray<int>) =

        let size = positionsForResCols.Length

        let update =
            <@
                fun (ndRange: Range1D)
                    (rowPtrsForCols: ClArray<int>)
                    (positionsForResCols: ClArray<int>)
                    (positions: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        positions.[positionsForResCols.[i]] <- rowPtrsForCols.[i]
            @>

        let kernel = clContext.CreateClKernel(update)
        let ndRange = Range1D.CreateValid(size, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        rowPtrsForCols
                        positionsForResCols
                        positions)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private getSecondPositions
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (rowPtrsForCols: ClArray<int>)
        (positionsForResCols: ClArray<int>)
        (headFlags: ClArray<int>)
        (resultMatrix: CSRMatrix<'a>) =

        let positions = ClArray.create clContext workGroupSize processor resultMatrix.Columns.Length 1
        updateSecondPositions clContext workGroupSize processor rowPtrsForCols positionsForResCols positions
        let resPositions = PrefixSum.byHeadFlagsInclude clContext workGroupSize processor headFlags positions <@ (+) @> 0
        processor.Post(Msg.CreateFreeMsg<_>(positions))
        resPositions

    let private updateFirstPositions
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (positionsForResCols: ClArray<int>)
        (positions: ClArray<int>) =

        let size = positionsForResCols.Length

        let update =
            <@
                fun (ndRange: Range1D)
                    (positionsForResCols: ClArray<int>)
                    (positions: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        positions.[positionsForResCols.[i]] <- i
            @>

        let kernel = clContext.CreateClKernel(update)
        let ndRange = Range1D.CreateValid(size, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        positionsForResCols
                        positions)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private getFirstPositions
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (positionsForResCols: ClArray<int>)
        (resultMatrix: CSRMatrix<'a>) =

        let positions = ClArray.create clContext workGroupSize processor resultMatrix.Columns.Length 0

        updateFirstPositions clContext workGroupSize processor positionsForResCols positions

        let max = <@ fun a b -> if a > b then a else b @>
        // let total = clContext.CreateClArray(1)
        let total = clContext.CreateClCell()
        PrefixSum.runIncludeInplace clContext workGroupSize processor positions total max 0
        |> ignore
        processor.Post(Msg.CreateFreeMsg(total))

        positions

    let private getResultHeadFlags
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (positionsForResCols: ClArray<int>)
        (size: int)
        (resultMatrix: CSRMatrix<'a>) =

        let headFlags = ClArray.create clContext workGroupSize processor resultMatrix.Columns.Length 0

        let init =
            <@
                fun (ndRange: Range1D)
                    (positionsForResCols: ClArray<int>)
                    (heads: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        heads.[positionsForResCols.[i]] <- 1
            @>

        let kernel = clContext.CreateClKernel(init)
        let ndRange = Range1D.CreateValid(size, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        positionsForResCols
                        headFlags)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

        headFlags

    let private initPositionsForResCols
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>)
        (matrixRight: CSRMatrix<'a>) =

        let positionsForResCols =
            clContext.CreateClArray(
                matrixLeft.Columns.Length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        let size = positionsForResCols.Length

        let init =
            <@
                fun (ndRange: Range1D)
                    (secondRowPointers: ClArray<int>)
                    (firstColumns: ClArray<int>)
                    (positionsForResCols: ClArray<int>) ->

                    let i = ndRange.GlobalID0 + 1

                    if i < size then
                        positionsForResCols.[i] <- secondRowPointers.[firstColumns.[i - 1] + 1] - secondRowPointers.[firstColumns.[i - 1]]
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
                        positionsForResCols)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))
        positionsForResCols

    let private updatePositionsForResCols
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>)
        (resultMatrix: CSRMatrix<'a>)
        (positionsForResCols: ClArray<int>) =

        let size = matrixLeft.RowCount

        let update =
            <@
                fun (ndRange: Range1D)
                    (firstRowPointers: ClArray<int>)
                    (resultRowPointers: ClArray<int>)
                    (positionsForResCols: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        let index = firstRowPointers.[i]
                        if index <> firstRowPointers.[i + 1] then
                            positionsForResCols.[index] <- resultRowPointers.[i]
            @>

        let kernel = clContext.CreateClKernel(update)
        let ndRange = Range1D.CreateValid(size, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        matrixLeft.RowPointers
                        resultMatrix.RowPointers
                        positionsForResCols)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private getPositionsForResCols
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (headFlags: ClArray<int>)
        (matrixLeft: CSRMatrix<'a>)
        (matrixRight: CSRMatrix<'a>)
        (resultMatrix: CSRMatrix<'a>) =

        let positionsForResCols = initPositionsForResCols clContext workGroupSize processor matrixLeft matrixRight
        updatePositionsForResCols clContext workGroupSize processor matrixLeft resultMatrix positionsForResCols
        let resPositionsForResCols = PrefixSum.byHeadFlagsInclude clContext workGroupSize processor headFlags positionsForResCols <@ (+) @> 0
        processor.Post(Msg.CreateFreeMsg<_>(positionsForResCols))
        resPositionsForResCols

    let private initRowPtrsForCols
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>)
        (matrixRight: CSRMatrix<'a>) =

        let rowPtrsForCols =
            clContext.CreateClArray(
                matrixLeft.Columns.Length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        let size = rowPtrsForCols.Length

        let init =
            <@
                fun (ndRange: Range1D)
                    (secondRowPointers: ClArray<int>)
                    (firstColumns: ClArray<int>)
                    (rowPtrsForCols: ClArray<int>) ->

                    let i = ndRange.GlobalID0 + 1

                    if i < size then
                        rowPtrsForCols.[i] <- secondRowPointers.[firstColumns.[i]] - secondRowPointers.[firstColumns.[i - 1]]
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
                        rowPtrsForCols)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

        rowPtrsForCols

    let private updateRowPtrsForCols
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>)
        (matrixRight: CSRMatrix<'a>)
        (rowPtrsForCols: ClArray<int>) =

        let size = matrixLeft.RowCount

        let update =
            <@
                fun (ndRange: Range1D)
                    (firstRowPointers: ClArray<int>)
                    (firstColumns: ClArray<int>)
                    (secondRowPointers: ClArray<int>)
                    (rowPtrsForCols: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        let index = firstRowPointers.[i]
                        if index <> firstRowPointers.[i + 1] then
                            rowPtrsForCols.[index] <- secondRowPointers.[firstColumns.[index]]
            @>

        let kernel = clContext.CreateClKernel(update)
        let ndRange = Range1D.CreateValid(size, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        matrixLeft.RowPointers
                        matrixLeft.Columns
                        matrixRight.RowPointers
                        rowPtrsForCols)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private getRowPtrsForCols
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (headFlags: ClArray<int>)
        (matrixLeft: CSRMatrix<'a>)
        (matrixRight: CSRMatrix<'a>) =

        let rowPtrsForCols = initRowPtrsForCols clContext workGroupSize processor matrixLeft matrixRight
        updateRowPtrsForCols clContext workGroupSize processor matrixLeft matrixRight rowPtrsForCols
        let resRowPtrsForCols = PrefixSum.byHeadFlagsInclude clContext workGroupSize processor headFlags rowPtrsForCols <@ (+) @> 0
        processor.Post(Msg.CreateFreeMsg<_>(rowPtrsForCols))
        resRowPtrsForCols

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

    let run
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>)
        (matrixRight: CSRMatrix<'a>)
        (resultMatrix: CSRMatrix<'a>)
        (times: Expr<'a -> 'a -> 'a>) =

        let headFlags = getHeadFlags clContext workGroupSize processor matrixLeft

        let rowPtrsForCols = getRowPtrsForCols clContext workGroupSize processor headFlags matrixLeft matrixRight
        let positionsForResCols = getPositionsForResCols clContext workGroupSize processor headFlags matrixLeft matrixRight resultMatrix
        processor.Post(Msg.CreateFreeMsg<_>(headFlags))

        let resultHeadFlags = getResultHeadFlags clContext workGroupSize processor positionsForResCols matrixLeft.Columns.Length resultMatrix

        let secondPositions = getSecondPositions clContext workGroupSize processor rowPtrsForCols positionsForResCols resultHeadFlags resultMatrix
        processor.Post(Msg.CreateFreeMsg<_>(rowPtrsForCols))
        processor.Post(Msg.CreateFreeMsg<_>(resultHeadFlags))
        let firstPositions = getFirstPositions clContext workGroupSize processor positionsForResCols resultMatrix
        processor.Post(Msg.CreateFreeMsg<_>(positionsForResCols))

        let resultColumns = Gather.run clContext workGroupSize processor secondPositions matrixRight.Columns
        let firstValues = Gather.run clContext workGroupSize processor firstPositions matrixLeft.Values
        processor.Post(Msg.CreateFreeMsg<_>(firstPositions))
        let secondValues = Gather.run clContext workGroupSize processor secondPositions matrixRight.Values
        processor.Post(Msg.CreateFreeMsg<_>(secondPositions))

        let resultValues = ClArray.zipWith clContext workGroupSize processor times firstValues secondValues
        processor.Post(Msg.CreateFreeMsg<_>(firstValues))
        processor.Post(Msg.CreateFreeMsg<_>(secondValues))

        {
            RowCount = resultMatrix.RowCount
            ColumnCount = resultMatrix.ColumnCount
            RowPointers = resultMatrix.RowPointers
            Columns = resultColumns
            Values = resultValues
        }
