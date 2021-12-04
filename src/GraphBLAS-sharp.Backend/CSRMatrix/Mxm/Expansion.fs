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
        let p2' = getP2' clContext workGroupSize processor headFlags matrixLeft matrixRight resultMatrix

        let resultHeadFlags = getResultHeadFlags clContext workGroupSize processor p2' resultMatrix

        let secondPositions = getSecondPositions clContext workGroupSize processor p2 p2' resultHeadFlags resultMatrix
        let firstPositions = getFirstPositions clContext workGroupSize processor p2' resultHeadFlags resultMatrix

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

    // TODO: инициализировать нулями
    let private getFirstPositions
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (p2': ClArray<int>)
        (headFlags: ClArray<int>)
        (resultMatrix: CSRMatrix<'a>) =

        let positions =
            clContext.CreateClArray(
                resultMatrix.Columns.Length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        updateFirstPositions clContext workGroupSize processor p2' positions
        let max = <@ fun a b -> if a > b then a else b @>
        ByHeadFlags.runInclude clContext workGroupSize processor headFlags positions max 0


    let private updateFirstPositions
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (p2': ClArray<int>)
        (positions: ClArray<int>) =

        let size = p2'.Length

        let update =
            <@
                fun (ndRange: Range1D)
                    (p2': ClArray<int>)
                    (positions: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        positions.[p2'.[i]] <- i
            @>

        let kernel = clContext.CreateClKernel(update)
        let ndRange = Range1D.CreateValid(size, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        p2'
                        positions)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private getSecondPositions
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (p2: ClArray<int>)
        (p2': ClArray<int>)
        (headFlags: ClArray<int>)
        (resultMatrix: CSRMatrix<'a>) =

        let positions = initSecondPositions clContext workGroupSize processor resultMatrix
        updateSecondPositions clContext workGroupSize processor p2 p2' positions
        ByHeadFlags.runInclude clContext workGroupSize processor headFlags positions <@ (+) @> 0

    // TODO: инициализировать нулями
    let private getResultHeadFlags
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (p2': ClArray<int>)
        (resultMatrix: CSRMatrix<'a>) =

        let headFlags =
            clContext.CreateClArray(
                resultMatrix.Columns.Length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        let size = resultMatrix.RowCount

        let init =
            <@
                fun (ndRange: Range1D)
                    (p2': ClArray<int>)
                    (heads: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        heads.[p2'.[i]] <- 1
            @>

        let kernel = clContext.CreateClKernel(init)
        let ndRange = Range1D.CreateValid(size, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        p2'
                        headFlags)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

        headFlags

    let private updateSecondPositions
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (p2: ClArray<int>)
        (p2': ClArray<int>)
        (positions: ClArray<int>) =

        let size = p2'.Length

        let update =
            <@
                fun (ndRange: Range1D)
                    (p2: ClArray<int>)
                    (p2': ClArray<int>)
                    (positions: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        positions.[p2'.[i]] <- p2.[i]
            @>

        let kernel = clContext.CreateClKernel(update)
        let ndRange = Range1D.CreateValid(size, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        p2
                        p2'
                        positions)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private initSecondPositions
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (resultMatrix: CSRMatrix<'a>) =

        let positions =
            clContext.CreateClArray(
                resultMatrix.Columns.Length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        let size = positions.Length

        let init =
            <@
                fun (ndRange: Range1D)
                    (positions: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        positions.[i] <- 1
            @>

        let kernel = clContext.CreateClKernel(init)
        let ndRange = Range1D.CreateValid(size, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        positions)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

        positions

    // TODO: инициализировать нулями в начале
    let private getHeadFlags
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>) =

        let headFlags =
            clContext.CreateClArray(
                matrixLeft.Columns.Length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

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
        ByHeadFlags.runInclude clContext workGroupSize processor headFlags p2 <@ (+) @> 0

    let private getP2'
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (headFlags: ClArray<int>)
        (matrixLeft: CSRMatrix<'a>)
        (matrixRight: CSRMatrix<'a>)
        (resultMatrix: CSRMatrix<'a>) =

        let p2' = initP2' clContext workGroupSize processor matrixLeft matrixRight
        updateP2' clContext workGroupSize processor matrixLeft resultMatrix p2'
        ByHeadFlags.runInclude clContext workGroupSize processor headFlags p2' <@ (+) @> 0

    let private initP2'
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>)
        (matrixRight: CSRMatrix<'a>) =

        let p2' =
            clContext.CreateClArray(
                matrixLeft.Columns.Length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        let size = p2'.Length

        let init =
            <@
                fun (ndRange: Range1D)
                    (secondRowPointers: ClArray<int>)
                    (firstColumns: ClArray<int>)
                    (p2': ClArray<int>) ->

                    let i = ndRange.GlobalID0 + 1

                    if i < size then
                        p2'.[i] <- secondRowPointers.[firstColumns.[i - 1] + 1] - secondRowPointers.[firstColumns.[i - 1]]
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
                        p2')
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))
        p2'

    let private updateP2'
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>)
        (resultMatrix: CSRMatrix<'a>)
        (p2': ClArray<int>) =

        let size = matrixLeft.RowCount

        let update =
            <@
                fun (ndRange: Range1D)
                    (firstRowPointers: ClArray<int>)
                    (resultRowPointers: ClArray<int>)
                    (p2': ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        let index = firstRowPointers.[i]
                        if index <> firstRowPointers.[i + 1] then
                            p2'.[index] <- resultRowPointers.[i]
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
                        p2')
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

    // let private preparePositions
    //     (clContext: ClContext)
    //     workGroupSize
    //     (processor: MailboxProcessor<_>)
    //     (matrixLeft: CSRMatrix<'a>)
    //     (matrixRight: CSRMatrix<'a>)
    //     (resultMatrix: CSRMatrix<'a>) =

    //     let positions =
    //             clContext.CreateClArray(
    //                 matrixLeft.Columns.Length,
    //                 hostAccessMode = HostAccessMode.NotAccessible
    //             )

    //     let heads =
    //         clContext.CreateClArray(
    //             matrixLeft.Columns.Length,
    //             hostAccessMode = HostAccessMode.NotAccessible
    //         )

    //     initHeads clContext workGroupSize processor matrixLeft.RowPointers heads

    //     initPositions clContext workGroupSize processor matrixRight.RowPointers matrixLeft.Columns positions
    //     let secondRowPtrsForFirstCols = ClArray.copy clContext processor workGroupSize positions

    //     updatePositions clContext workGroupSize processor matrixLeft.RowPointers resultMatrix.RowPointers positions
    //     updateSecondRowPtrsForFirstCols clContext workGroupSize processor matrixLeft.RowPointers matrixLeft.Columns matrixRight.RowPointers secondRowPtrsForFirstCols

    //     ()

    // let private initPositions
    //     (clContext: ClContext)
    //     workGroupSize
    //     (processor: MailboxProcessor<_>)
    //     (secondRowPointers: ClArray<int>)
    //     (firstColumns: ClArray<int>)
    //     (positions: ClArray<int>) =

    //     let size = positions.Length - 1

    //     let init =
    //         <@
    //             fun (ndRange: Range1D)
    //                 (secondRowPointers: ClArray<int>)
    //                 (firstColumns: ClArray<int>)
    //                 (positions: ClArray<int>) ->

    //                 let i = ndRange.GlobalID0

    //                 if i < size then
    //                     positions.[i] <- secondRowPointers.[firstColumns.[i + 1]] - secondRowPointers.[firstColumns.[i]]
    //         @>

    //     let kernel = clContext.CreateClKernel(init)

    //     let ndRange = Range1D.CreateValid(size, workGroupSize)

    //     processor.Post(
    //         Msg.MsgSetArguments
    //             (fun () ->
    //                 kernel.ArgumentsSetter
    //                     ndRange
    //                     secondRowPointers
    //                     firstColumns
    //                     positions)
    //     )

    //     processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    // let private updatePositions
    //     (clContext: ClContext)
    //     workGroupSize
    //     (processor: MailboxProcessor<_>)
    //     (firstRowPointers: ClArray<int>)
    //     (resultRowPointers: ClArray<int>)
    //     (positions: ClArray<int>) =

    //     let size = firstRowPointers.Length - 1

    //     let update =
    //         <@
    //             fun (ndRange: Range1D)
    //                 (firstRowPointers: ClArray<int>)
    //                 (resultRowPointers: ClArray<int>)
    //                 (positions: ClArray<int>) ->

    //                 let i = ndRange.GlobalID0

    //                 if i < size then
    //                     let index = firstRowPointers.[i]
    //                     if index <> firstRowPointers.[i + 1] then
    //                         positions.[index] <- positions.[index] + resultRowPointers.[i]
    //         @>

    //     let kernel = clContext.CreateClKernel(update)

    //     let ndRange = Range1D.CreateValid(size, workGroupSize)

    //     processor.Post(
    //         Msg.MsgSetArguments
    //             (fun () ->
    //                 kernel.ArgumentsSetter
    //                     ndRange
    //                     firstRowPointers
    //                     resultRowPointers
    //                     positions)
    //     )

    //     processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    // let private updateSecondRowPtrsForFirstCols
    //     (clContext: ClContext)
    //     workGroupSize
    //     (processor: MailboxProcessor<_>)
    //     (firstRowPointers: ClArray<int>)
    //     (firstColumns: ClArray<int>)
    //     (secondRowPointers: ClArray<int>)
    //     (secondRowPtrsForFirstCols: ClArray<int>) =

    //     let firstRowPointersCompressed = ClArray.removeDuplications clContext workGroupSize processor firstRowPointers

    //     let size = firstRowPointersCompressed.Length - 1

    //     let update =
    //         <@
    //             fun (ndRange: Range1D)
    //                 (firstRowPointersCompressed: ClArray<int>)
    //                 (firstColumns: ClArray<int>)
    //                 (secondRowPointers: ClArray<int>)
    //                 (secondRowPtrsForFirstCols: ClArray<int>) ->

    //                 let i = ndRange.GlobalID0

    //                 if i < size then
    //                     let index = firstRowPointersCompressed.[i]
    //                     secondRowPtrsForFirstCols.[index] <-
    //                         secondRowPtrsForFirstCols.[index] + secondRowPointers.[firstColumns.[index]]
    //         @>

    //     let kernel = clContext.CreateClKernel(update)

    //     let ndRange = Range1D.CreateValid(size, workGroupSize)

    //     processor.Post(
    //         Msg.MsgSetArguments
    //             (fun () ->
    //                 kernel.ArgumentsSetter
    //                     ndRange
    //                     firstRowPointersCompressed
    //                     firstColumns
    //                     secondRowPointers
    //                     secondRowPtrsForFirstCols)
    //     )

    //     processor.Post(Msg.CreateRunMsg<_, _>(kernel))


    // let run (clContext: ClContext) workGroupSize =
    //     fun (processor: MailboxProcessor<_>) (matrixLeft: CSRMatrix<'a>) (matrixRight: CSRMatrix<'a>) (resultMatrix: CSRMatrix<'a>) (times: Expr<'a -> 'a -> 'a>) ->
    //         let rowCount = matrixLeft.RowCount

    //         let expand =
    //             <@
    //                 fun (ndRange: Range1D)
    //                     (firstRowPointersBuffer: ClArray<int>)
    //                     (firstColumnsBuffer: ClArray<int>)
    //                     (firstValuesBuffer: ClArray<'a>)
    //                     (secondRowPointersBuffer: ClArray<int>)
    //                     (secondColumnsBuffer: ClArray<int>)
    //                     (secondValuesBuffer: ClArray<'a>)
    //                     (resultRowPointersBuffer: ClArray<int>)
    //                     (resultColumnsBuffer: ClArray<int>)
    //                     (resultValuesBuffer: ClArray<'a>) ->

    //                     let i = ndRange.GlobalID0

    //                     if i < rowCount then
    //                         let mutable bound = resultRowPointersBuffer.[i]
    //                         for k in firstRowPointersBuffer.[i] .. firstRowPointersBuffer.[i + 1] - 1 do
    //                             let colIdx = firstColumnsBuffer.[k]
    //                             let value = firstValuesBuffer.[k]
    //                             for j in secondRowPointersBuffer.[colIdx] .. secondRowPointersBuffer.[colIdx + 1] - 1 do
    //                                 resultColumnsBuffer.[bound] <- secondColumnsBuffer.[j]
    //                                 resultValuesBuffer.[bound] <- (%times) value secondValuesBuffer.[j]
    //                                 bound <- bound + 1
    //             @>

    //         let kernel = clContext.CreateClKernel(expand)

    //         let ndRange = Range1D.CreateValid(matrixLeft.RowCount, workGroupSize)

    //         processor.Post(
    //             Msg.MsgSetArguments
    //                 (fun () ->
    //                     kernel.ArgumentsSetter
    //                         ndRange
    //                         matrixLeft.RowPointers
    //                         matrixLeft.Columns
    //                         matrixLeft.Values
    //                         matrixRight.RowPointers
    //                         matrixRight.Columns
    //                         matrixRight.Values
    //                         resultMatrix.RowPointers
    //                         resultMatrix.Columns
    //                         resultMatrix.Values)
    //         )

    //         processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    //         resultMatrix
