namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open Microsoft.FSharp.Quotations

module CSRMatrix =
    let expandRows (clContext: ClContext) =
        let expandRows =
            <@ fun (range: Range1D) workGroupSize (rowPointers: ClArray<int>) (rowIndices: ClArray<int>) ->

                let lid = range.LocalID0
                let groupId = range.GlobalID0 / workGroupSize

                let rowStart = rowPointers.[groupId]
                let rowEnd = rowPointers.[groupId + 1]
                let rowLength = rowEnd - rowStart

                let mutable i = lid

                while i < rowLength do
                    rowIndices.[rowStart + i] <- groupId
                    i <- i + workGroupSize @>

        let kernel = clContext.CreateClKernel expandRows

        fun (processor: MailboxProcessor<_>) workGroupSize rowPointers rowCount (nnz: int) ->
            let ndRange =
                Range1D.CreateValid(rowCount * workGroupSize, workGroupSize)

            let rowIndices =
                clContext.CreateClArray(
                    nnz,
                    hostAccessMode = HostAccessMode.ReadWrite,
                    deviceAccessMode = DeviceAccessMode.ReadWrite
                )

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.SetArguments ndRange workGroupSize rowPointers rowIndices)
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

    let eWiseAdd (clContext: ClContext) (opAdd: Expr<'a -> 'a -> 'a>) workGroupSize =

        let compressRows =
            COOMatrix.compressRows clContext workGroupSize

        let eWiseCOO =
            COOMatrix.eWiseAdd clContext opAdd workGroupSize

        let expandRows = expandRows clContext

        fun (processor: MailboxProcessor<_>) (m1: CSRMatrix<'a>) (m2: CSRMatrix<'a>) ->

            let m1Rows =
                expandRows processor workGroupSize m1.RowPointers m1.RowCount m1.Values.Length

            let m1COO =
                { Context = clContext
                  RowCount = m1.RowCount
                  ColumnCount = m1.ColumnCount
                  Columns = m1.Columns
                  Rows = m1Rows
                  Values = m1.Values }

            let m2Rows =
                expandRows processor workGroupSize m2.RowPointers m2.RowCount m2.Values.Length

            let m2COO =
                { Context = clContext
                  RowCount = m2.RowCount
                  ColumnCount = m2.ColumnCount
                  Columns = m2.Columns
                  Rows = m2Rows
                  Values = m2.Values }

            let m3COO = eWiseCOO processor m1COO m2COO

            processor.Post(Msg.CreateFreeMsg(m1COO.Rows))
            processor.Post(Msg.CreateFreeMsg(m2COO.Rows))

            let m3RowPointers =
                compressRows processor m3COO.Rows m3COO.RowCount

            let m3 =
                { Context = clContext
                  RowCount = m3COO.RowCount
                  ColumnCount = m3COO.ColumnCount
                  Columns = m3COO.Columns
                  RowPointers = m3RowPointers
                  Values = m3COO.Values }

            processor.Post(Msg.CreateFreeMsg(m3COO.Rows))

            m3

    let eWiseAdd2 (clContext: ClContext) (opAdd: Expr<'a -> 'a -> 'a>) workGroupSize =

        let toCOOInplace = toCOOInplace clContext workGroupSize

        let eWiseCOO =
            COOMatrix.eWiseAdd clContext opAdd workGroupSize

        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (m1: CSRMatrix<'a>) (m2: CSRMatrix<'a>) ->

            let m1COO = toCOOInplace processor m1
            let m2COO = toCOOInplace processor m2

            let m3COO = eWiseCOO processor m1COO m2COO

            processor.Post(Msg.CreateFreeMsg(m1COO.Rows))
            processor.Post(Msg.CreateFreeMsg(m2COO.Rows))

            let m3 = toCSRInplace processor m3COO
            processor.Post(Msg.CreateFreeMsg(m3COO.Rows))

            m3
