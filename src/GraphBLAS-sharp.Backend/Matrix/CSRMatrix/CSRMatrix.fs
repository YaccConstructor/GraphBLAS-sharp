namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open Microsoft.FSharp.Quotations

module CSRMatrix =
    let toCOO (clContext: ClContext) =

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
        let copy = ClArray.copy clContext
        let copyData = ClArray.copy clContext

        fun (processor: MailboxProcessor<_>) workGroupSize (matrix: CSRMatrix<'a>) ->
            let ndRange =
                Range1D.CreateValid(matrix.RowCount * workGroupSize, workGroupSize)

            let rowIndices =
                clContext.CreateClArray matrix.Values.Length

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.SetArguments ndRange workGroupSize matrix.RowPointers rowIndices)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            let colIndices =
                copy processor workGroupSize matrix.Columns

            let values =
                copyData processor workGroupSize matrix.Values

            { RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = rowIndices
              Columns = colIndices
              Values = values }

    let eWiseAdd (clContext: ClContext) (opAdd: Expr<'a -> 'a -> 'a>) workGroupSize =

        let toCOO = toCOO clContext

        let eWiseCOO =
            COOMatrix.eWiseAdd clContext opAdd workGroupSize

        let toCSR = COOMatrix.toCSR clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (m1: CSRMatrix<'a>) (m2: CSRMatrix<'a>) ->

            let m1COO = toCOO processor workGroupSize m1
            let m2COO = toCOO processor workGroupSize m2

            let m3COO = eWiseCOO processor m1COO m2COO

            let m3 = toCSR processor m3COO

            m3
