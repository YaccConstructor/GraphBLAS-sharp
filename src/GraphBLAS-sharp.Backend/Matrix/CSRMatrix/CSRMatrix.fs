namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Quotations

module CSRMatrix =
    let private expandRows (clContext: ClContext) =
        let expandRows =
            <@ fun (range: Range1D) workGroupSize (rowPointers: ClArray<int>) (rowIndices: ClArray<int>) rowCount nnz ->

                let lid = range.LocalID0
                let groupId = range.GlobalID0 / workGroupSize

                let rowStart = rowPointers.[groupId]

                let rowEnd =
                    if groupId <> rowCount - 1 then
                        rowPointers.[groupId + 1]
                    else
                        nnz

                let rowLength = rowEnd - rowStart

                let mutable i = lid

                while i < rowLength do
                    rowIndices.[rowStart + i] <- groupId
                    i <- i + workGroupSize @>

        let kernel = clContext.Compile(expandRows)

        fun (processor: MailboxProcessor<_>) workGroupSize rowPointers rowCount (nnz: int) ->
            let ndRange =
                Range1D.CreateValid(rowCount * workGroupSize, workGroupSize)

            let rowIndices =
                clContext.CreateClArray(
                    nnz,
                    hostAccessMode = HostAccessMode.ReadWrite,
                    deviceAccessMode = DeviceAccessMode.ReadWrite
                )

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange workGroupSize rowPointers rowIndices rowCount nnz)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            rowIndices

    let toCOO (clContext: ClContext) workGroupSize =

        let expandRows = expandRows clContext
        let copy = ClArray.copy clContext
        let copyData = ClArray.copy clContext

        fun (processor: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) ->

            let rowIndices =
                expandRows processor workGroupSize matrix.RowPointers matrix.RowCount matrix.Values.Length

            let colIndices =
                copy processor workGroupSize matrix.Columns

            let values =
                copyData processor workGroupSize matrix.Values

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = rowIndices
              Columns = colIndices
              Values = values }

    let toCOOInplace (clContext: ClContext) workGroupSize =

        let expandRows = expandRows clContext

        fun (processor: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) ->

            let rowIndices =
                expandRows processor workGroupSize matrix.RowPointers matrix.RowCount matrix.Values.Length

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = rowIndices
              Columns = matrix.Columns
              Values = matrix.Values }

    let eWiseAdd (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) workGroupSize =

        let toCOOInplaceLeft = toCOOInplace clContext workGroupSize
        let toCOOInplaceRight = toCOOInplace clContext workGroupSize

        let eWiseCOO =
            COOMatrix.eWiseAdd clContext opAdd workGroupSize

        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (m1: CSRMatrix<'a>) (m2: CSRMatrix<'b>) ->

            let m1COO = toCOOInplaceLeft processor m1
            let m2COO = toCOOInplaceRight processor m2

            let m3COO = eWiseCOO processor m1COO m2COO

            processor.Post(Msg.CreateFreeMsg(m1COO.Rows))
            processor.Post(Msg.CreateFreeMsg(m2COO.Rows))

            let m3 = toCSRInplace processor m3COO
            processor.Post(Msg.CreateFreeMsg(m3COO.Rows))

            m3

    let eWiseAdd2 (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) workGroupSize =

        let toCOOInplaceLeft = toCOOInplace clContext workGroupSize
        let toCOOInplaceRight = toCOOInplace clContext workGroupSize

        let eWiseCOO =
            COOMatrix.eWiseAdd2 clContext opAdd workGroupSize

        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (m1: CSRMatrix<'a>) (m2: CSRMatrix<'b>) ->

            let m1COO = toCOOInplaceLeft processor m1
            let m2COO = toCOOInplaceRight processor m2

            let m3COO = eWiseCOO processor m1COO m2COO

            processor.Post(Msg.CreateFreeMsg(m1COO.Rows))
            processor.Post(Msg.CreateFreeMsg(m2COO.Rows))

            let m3 = toCSRInplace processor m3COO
            processor.Post(Msg.CreateFreeMsg(m3COO.Rows))

            m3
