namespace GraphBLAS.FSharp.Backend.Matrix.CSR

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Quotes
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClMatrix
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ClVector
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions


module Matrix =
    let expandRowPointers (clContext: ClContext) workGroupSize =

        let kernel =
            <@ fun (ndRange: Range1D) columnsLength pointersLength (pointers: ClArray<int>) (results: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                if gid < columnsLength then
                    let result =
                        (%Search.Bin.lowerBound) pointersLength gid pointers

                    results.[gid] <- result - 1 @>

        let program = clContext.Compile kernel

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->

            let rows =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, matrix.Columns.Length)

            let kernel = program.GetKernel()

            let ndRange =
                Range1D.CreateValid(matrix.Columns.Length, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            matrix.Columns.Length
                            matrix.RowPointers.Length
                            matrix.RowPointers
                            rows)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            rows

    let item<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let kernel =
            <@ fun (ndRange: Range1D) row column (rowPointers: ClArray<int>) (columns: ClArray<int>) (values: ClArray<'a>) (result: ClCell<'a option>) ->

                let gid = ndRange.GlobalID0

                if gid = 0 then
                    let firstIndex = rowPointers.[row]
                    let lastIndex = rowPointers.[row + 1] - 1

                    result.Value <- (%Search.Bin.inRange) firstIndex lastIndex column columns values @>

        let program = clContext.Compile kernel

        fun (processor: MailboxProcessor<_>) (row: int) (column: int) (matrix: ClMatrix.CSR<'a>) ->

            if row < 0 || row >= matrix.RowCount then
                failwith "Row out of range"

            if column < 0 || column >= matrix.ColumnCount then
                failwith "Column out of range"

            let result = clContext.CreateClCell None

            let kernel = program.GetKernel()

            let ndRange = Range1D.CreateValid(1, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc ndRange row column matrix.RowPointers matrix.Columns matrix.Values result)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            result

    let subRows (clContext: ClContext) workGroupSize =

        let kernel =
            <@ fun (ndRange: Range1D) resultLength sourceRow pointersLength (pointers: ClArray<int>) (results: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                let shift = pointers.[sourceRow]
                let shiftedId = gid + shift

                if gid < resultLength then
                    let result =
                        (%Search.Bin.lowerBound) pointersLength shiftedId pointers

                    results.[gid] <- result - 1 @>

        let program = clContext.Compile kernel

        let blit = ClArray.blit clContext workGroupSize

        let blitData = ClArray.blit clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode startIndex count (matrix: ClMatrix.CSR<'a>) ->
            if count <= 0 then
                failwith "Count must be greater than zero"

            if startIndex < 0 then
                failwith "startIndex must be greater then zero"

            if startIndex + count > matrix.RowCount then
                failwith "startIndex and count sum is larger than the matrix row count"

            // extract rows
            let rowPointers = matrix.RowPointers.ToHost processor

            let resultLength =
                rowPointers.[startIndex + count]
                - rowPointers.[startIndex]

            let rows =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

            let kernel = program.GetKernel()

            let ndRange =
                Range1D.CreateValid(matrix.Columns.Length, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            resultLength
                            startIndex
                            matrix.RowPointers.Length
                            matrix.RowPointers
                            rows)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            let startPosition = rowPointers.[startIndex]

            // extract values
            let values =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

            blitData processor matrix.Values startPosition values 0 resultLength

            // extract indices
            let columns =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

            blit processor matrix.Columns startPosition columns 0 resultLength

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = rows
              Columns = columns
              Values = values }

    let toCOO (clContext: ClContext) workGroupSize =
        let prepare =
            expandRowPointers clContext workGroupSize

        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->
            let rows = prepare processor allocationMode matrix

            let cols =
                copy processor allocationMode matrix.Columns

            let values =
                copyData processor allocationMode matrix.Values

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = rows
              Columns = cols
              Values = values }

    let toCOOInPlace (clContext: ClContext) workGroupSize =
        let prepare =
            expandRowPointers clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->
            let rows = prepare processor allocationMode matrix

            processor.Post(Msg.CreateFreeMsg(matrix.RowPointers))

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = rows
              Columns = matrix.Columns
              Values = matrix.Values }

    let map = CSR.Map.run

    let map2 = Map2.run

    let map2AtLeastOne<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        Map2.AtLeastOne.run (Convert.atLeastOneToOption opAdd) clContext workGroupSize

    let transposeInPlace (clContext: ClContext) workGroupSize =

        let toCOOInPlace = toCOOInPlace clContext workGroupSize

        let transposeInPlace =
            COO.Matrix.transposeInPlace clContext workGroupSize

        let toCSRInPlace =
            COO.Matrix.toCSRInPlace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->
            toCOOInPlace queue allocationMode matrix
            |> transposeInPlace queue
            |> toCSRInPlace queue allocationMode

    let transpose (clContext: ClContext) workGroupSize =

        let toCOO = toCOO clContext workGroupSize

        let transposeInPlace =
            COO.Matrix.transposeInPlace clContext workGroupSize

        let toCSRInPlace =
            COO.Matrix.toCSRInPlace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->
            toCOO queue allocationMode matrix
            |> transposeInPlace queue
            |> toCSRInPlace queue allocationMode

    let byRowsLazy (clContext: ClContext) workGroupSize =

        let getChunkValues = ClArray.sub clContext workGroupSize

        let getChunkIndices = ClArray.sub clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->

            let getChunkValues =
                getChunkValues processor allocationMode matrix.Values

            let getChunkIndices =
                getChunkIndices processor allocationMode matrix.Columns

            let creatSparseVector values columns =
                { Context = clContext
                  Indices = columns
                  Values = values
                  Size = matrix.ColumnCount }

            matrix.RowPointers.ToHost processor
            |> Seq.pairwise
            |> Seq.map
                (fun (first, second) ->
                    lazy
                        (let count = second - first

                         if count > 0 then
                             let values = getChunkValues first count
                             let columns = getChunkIndices first count

                             Some <| creatSparseVector values columns
                         else
                             None))

    let byRows (clContext: ClContext) workGroupSize =

        let runLazy = byRowsLazy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->
            runLazy processor allocationMode matrix
            |> Seq.map (fun lazyValue -> lazyValue.Value)

    let toLIL (clContext: ClContext) workGroupSize =

        let byRows = byRows clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->
            let rows =
                byRows processor allocationMode matrix
                |> Seq.toList

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = rows
              NNZ = matrix.NNZ }

    let NNZInRows (clContext: ClContext) workGroupSize =

        let pairwise = ClArray.pairwise clContext workGroupSize

        let subtract =
            Map.map <@ fun (fst, snd) -> snd - fst @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'b>) ->
            let pointerPairs =
                pairwise processor DeviceOnly matrix.RowPointers
                // since row pointers length in matrix always >= 2
                |> Option.defaultWith
                    (fun () -> failwith "The state of the matrix is broken. The length of the rowPointers must be >= 2")

            let rowsLength =
                subtract processor allocationMode pointerPairs

            pointerPairs.Free processor

            rowsLength

    let kronecker = Kronecker.run
