namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Predefined

module internal Expansion =
    let private updateSecondPositions
        (clContext: ClContext)
        workGroupSize =

        let update =
            <@
                fun (ndRange: Range1D)
                    (rowPtrsForCols: ClArray<int>)
                    (positionsForResCols: ClArray<int>)
                    (size: int)
                    (positions: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        positions.[positionsForResCols.[i]] <- rowPtrsForCols.[i]
            @>
        let kernel = clContext.CreateClKernel(update)

        fun (processor: MailboxProcessor<_>)
            (rowPtrsForCols: ClArray<int>)
            (positionsForResCols: ClArray<int>)
            (positions: ClArray<int>) ->

            let size = positionsForResCols.Length

            let ndRange = Range1D.CreateValid(size, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.ArgumentsSetter
                            ndRange
                            rowPtrsForCols
                            positionsForResCols
                            size
                            positions)
            )
            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private getSecondPositions
        (clContext: ClContext)
        workGroupSize =

        let create = ClArray.create clContext workGroupSize
        let updateSecondPositions = updateSecondPositions clContext workGroupSize
        let scanByHeadFlagsInclude = PrefixSum.byHeadFlagsInclude <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (rowPtrsForCols: ClArray<int>)
            (positionsForResCols: ClArray<int>)
            (headFlags: ClArray<int>)
            (resultMatrix: CSRMatrix<'a>) ->

            let positions = create processor resultMatrix.Columns.Length 1
            updateSecondPositions processor rowPtrsForCols positionsForResCols positions
            let resPositions = scanByHeadFlagsInclude processor headFlags positions 0
            processor.Post(Msg.CreateFreeMsg<_>(positions))
            resPositions

    let private updateFirstPositions
        (clContext: ClContext)
        workGroupSize =

        let update =
            <@
                fun (ndRange: Range1D)
                    (positionsForResCols: ClArray<int>)
                    (size: int)
                    (positions: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        positions.[positionsForResCols.[i]] <- i
            @>
        let kernel = clContext.CreateClKernel(update)

        fun (processor: MailboxProcessor<_>)
            (positionsForResCols: ClArray<int>)
            (positions: ClArray<int>) ->

            let size = positionsForResCols.Length

            let ndRange = Range1D.CreateValid(size, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.ArgumentsSetter
                            ndRange
                            positionsForResCols
                            size
                            positions)
            )
            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private getFirstPositions
        (clContext: ClContext)
        workGroupSize =

        let create = ClArray.create clContext workGroupSize
        let updateFirstPositions = updateFirstPositions clContext workGroupSize
        let max = <@ fun a b -> if a > b then a else b @>
        let scanIncludeInPlace = PrefixSum.runIncludeInplace max clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (positionsForResCols: ClArray<int>)
            (resultMatrix: CSRMatrix<'a>) ->

            let positions = create processor resultMatrix.Columns.Length 0

            updateFirstPositions processor positionsForResCols positions

            let total = clContext.CreateClCell()
            scanIncludeInPlace processor positions total 0
            |> ignore
            processor.Post(Msg.CreateFreeMsg(total))

            positions

    let private getResultHeadFlags
        (clContext: ClContext)
        workGroupSize =

        let create = ClArray.create clContext workGroupSize

        let init =
            <@
                fun (ndRange: Range1D)
                    (positionsForResCols: ClArray<int>)
                    (size: int)
                    (heads: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        heads.[positionsForResCols.[i]] <- 1
            @>
        let kernel = clContext.CreateClKernel(init)

        fun (processor: MailboxProcessor<_>)
            (positionsForResCols: ClArray<int>)
            (size: int)
            (resultMatrix: CSRMatrix<'a>) ->

            let headFlags = create processor resultMatrix.Columns.Length 0

            let ndRange = Range1D.CreateValid(size, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.ArgumentsSetter
                            ndRange
                            positionsForResCols
                            size
                            headFlags)
            )
            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            headFlags

    let private initPositionsForResCols
        (clContext: ClContext)
        workGroupSize =

        let init =
            <@
                fun (ndRange: Range1D)
                    (secondRowPointers: ClArray<int>)
                    (firstColumns: ClArray<int>)
                    (positionsForResCols: ClArray<int>)
                    (size: int) ->

                    let i = ndRange.GlobalID0 + 1

                    if i < size then
                        positionsForResCols.[i] <- secondRowPointers.[firstColumns.[i - 1] + 1] - secondRowPointers.[firstColumns.[i - 1]]
            @>
        let kernel = clContext.CreateClKernel(init)

        fun (processor: MailboxProcessor<_>)
            (matrixLeft: CSRMatrix<'a>)
            (matrixRight: CSRMatrix<'a>) ->

            let positionsForResCols =
                clContext.CreateClArray(
                    matrixLeft.Columns.Length,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let size = positionsForResCols.Length

            let ndRange = Range1D.CreateValid(size - 1, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.ArgumentsSetter
                            ndRange
                            matrixRight.RowPointers
                            matrixLeft.Columns
                            positionsForResCols
                            size)
            )
            processor.Post(Msg.CreateRunMsg<_, _>(kernel))
            positionsForResCols

    let private updatePositionsForResCols
        (clContext: ClContext)
        workGroupSize =

        let update =
            <@
                fun (ndRange: Range1D)
                    (firstRowPointers: ClArray<int>)
                    (size: int)
                    (resultRowPointers: ClArray<int>)
                    (positionsForResCols: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        let index = firstRowPointers.[i]
                        if index <> firstRowPointers.[i + 1] then
                            positionsForResCols.[index] <- resultRowPointers.[i]
            @>
        let kernel = clContext.CreateClKernel(update)

        fun (processor: MailboxProcessor<_>)
            (matrixLeft: CSRMatrix<'a>)
            (resultMatrix: CSRMatrix<'a>)
            (positionsForResCols: ClArray<int>) ->

            let size = matrixLeft.RowCount

            let ndRange = Range1D.CreateValid(size, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.ArgumentsSetter
                            ndRange
                            matrixLeft.RowPointers
                            size
                            resultMatrix.RowPointers
                            positionsForResCols)
            )
            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private getPositionsForResCols
        (clContext: ClContext)
        workGroupSize =

        let initPositionsForResCols = initPositionsForResCols clContext workGroupSize
        let updatePositionsForResCols = updatePositionsForResCols clContext workGroupSize
        let scanByHeadFlagsInclude = PrefixSum.byHeadFlagsInclude <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (headFlags: ClArray<int>)
            (matrixLeft: CSRMatrix<'a>)
            (matrixRight: CSRMatrix<'a>)
            (resultMatrix: CSRMatrix<'a>) ->

            let positionsForResCols = initPositionsForResCols processor matrixLeft matrixRight
            updatePositionsForResCols processor matrixLeft resultMatrix positionsForResCols
            let resPositionsForResCols = scanByHeadFlagsInclude processor headFlags positionsForResCols 0
            processor.Post(Msg.CreateFreeMsg<_>(positionsForResCols))
            resPositionsForResCols

    let private initRowPtrsForCols
        (clContext: ClContext)
        workGroupSize =

        let init =
            <@
                fun (ndRange: Range1D)
                    (secondRowPointers: ClArray<int>)
                    (firstColumns: ClArray<int>)
                    (rowPtrsForCols: ClArray<int>)
                    (size: int) ->

                    let i = ndRange.GlobalID0 + 1

                    if i < size then
                        rowPtrsForCols.[i] <- secondRowPointers.[firstColumns.[i]] - secondRowPointers.[firstColumns.[i - 1]]
            @>
        let kernel = clContext.CreateClKernel(init)

        fun (processor: MailboxProcessor<_>)
            (matrixLeft: CSRMatrix<'a>)
            (matrixRight: CSRMatrix<'a>) ->

            let rowPtrsForCols =
                clContext.CreateClArray(
                    matrixLeft.Columns.Length,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let size = rowPtrsForCols.Length

            let ndRange = Range1D.CreateValid(size - 1, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.ArgumentsSetter
                            ndRange
                            matrixRight.RowPointers
                            matrixLeft.Columns
                            rowPtrsForCols
                            size)
            )
            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            rowPtrsForCols

    let private updateRowPtrsForCols
        (clContext: ClContext)
        workGroupSize =

        let update =
            <@
                fun (ndRange: Range1D)
                    (firstRowPointers: ClArray<int>)
                    (size: int)
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

        fun (processor: MailboxProcessor<_>)
            (matrixLeft: CSRMatrix<'a>)
            (matrixRight: CSRMatrix<'a>)
            (rowPtrsForCols: ClArray<int>) ->

            let size = matrixLeft.RowCount

            let ndRange = Range1D.CreateValid(size, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.ArgumentsSetter
                            ndRange
                            matrixLeft.RowPointers
                            size
                            matrixLeft.Columns
                            matrixRight.RowPointers
                            rowPtrsForCols)
            )
            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private getRowPtrsForCols
        (clContext: ClContext)
        workGroupSize =

        let initRowPtrsForCols = initRowPtrsForCols clContext workGroupSize
        let updateRowPtrsForCols = updateRowPtrsForCols clContext workGroupSize
        let scanByHeadFlagsInclude = PrefixSum.byHeadFlagsInclude <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (headFlags: ClArray<int>)
            (matrixLeft: CSRMatrix<'a>)
            (matrixRight: CSRMatrix<'a>) ->

            let rowPtrsForCols = initRowPtrsForCols processor matrixLeft matrixRight
            updateRowPtrsForCols processor matrixLeft matrixRight rowPtrsForCols
            let resRowPtrsForCols = scanByHeadFlagsInclude processor headFlags rowPtrsForCols 0
            processor.Post(Msg.CreateFreeMsg<_>(rowPtrsForCols))
            resRowPtrsForCols

    let private getHeadFlags
        (clContext: ClContext)
        workGroupSize =

        let create = ClArray.create clContext workGroupSize

        let init =
            <@
                fun (ndRange: Range1D)
                    (rowPointers: ClArray<int>)
                    (size: int)
                    (heads: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        heads.[rowPointers.[i]] <- 1
            @>
        let kernel = clContext.CreateClKernel(init)

        fun (processor: MailboxProcessor<_>)
            (matrixLeft: CSRMatrix<'a>) ->

            let headFlags = create processor matrixLeft.Columns.Length 0

            let size = matrixLeft.RowCount

            let ndRange = Range1D.CreateValid(size, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.ArgumentsSetter
                            ndRange
                            matrixLeft.RowPointers
                            size
                            headFlags)
            )
            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            headFlags

    let run
        (times: Expr<'a -> 'a -> 'a>)
        (clContext: ClContext)
        workGroupSize =

        let getHeadFlags = getHeadFlags clContext workGroupSize
        let getRowPtrsForCols = getRowPtrsForCols clContext workGroupSize
        let getPositionsForResCols = getPositionsForResCols clContext workGroupSize
        let getResultHeadFlags = getResultHeadFlags clContext workGroupSize
        let getFirstPositions = getFirstPositions clContext workGroupSize
        let getSecondPositions = getSecondPositions clContext workGroupSize
        let gather = Gather.run clContext workGroupSize
        let gatherData = Gather.run clContext workGroupSize
        let zipWith = ClArray.zipWith times clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (matrixLeft: CSRMatrix<'a>)
            (matrixRight: CSRMatrix<'a>)
            (resultMatrix: CSRMatrix<'a>) ->

            let headFlags = getHeadFlags processor matrixLeft

            let rowPtrsForCols = getRowPtrsForCols processor headFlags matrixLeft matrixRight
            let positionsForResCols = getPositionsForResCols processor headFlags matrixLeft matrixRight resultMatrix
            processor.Post(Msg.CreateFreeMsg<_>(headFlags))

            let resultHeadFlags = getResultHeadFlags processor positionsForResCols matrixLeft.Columns.Length resultMatrix

            let secondPositions = getSecondPositions processor rowPtrsForCols positionsForResCols resultHeadFlags resultMatrix
            processor.Post(Msg.CreateFreeMsg<_>(rowPtrsForCols))
            processor.Post(Msg.CreateFreeMsg<_>(resultHeadFlags))
            let firstPositions = getFirstPositions processor positionsForResCols resultMatrix
            processor.Post(Msg.CreateFreeMsg<_>(positionsForResCols))

            let resultColumns = gather processor secondPositions matrixRight.Columns
            let firstValues = gatherData processor firstPositions matrixLeft.Values
            processor.Post(Msg.CreateFreeMsg<_>(firstPositions))
            let secondValues = gatherData processor secondPositions matrixRight.Values
            processor.Post(Msg.CreateFreeMsg<_>(secondPositions))

            let resultValues = zipWith processor firstValues secondValues
            processor.Post(Msg.CreateFreeMsg<_>(firstValues))
            processor.Post(Msg.CreateFreeMsg<_>(secondValues))

            {
                RowCount = resultMatrix.RowCount
                ColumnCount = resultMatrix.ColumnCount
                RowPointers = resultMatrix.RowPointers
                Columns = resultColumns
                Values = resultValues
            }
