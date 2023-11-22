namespace GraphBLAS.FSharp.Backend.Matrix.COO

open Brahma.FSharp
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClMatrix
open GraphBLAS.FSharp.Objects.ClCellExtensions
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Objects.ClContextExtensions

module Matrix =
    /// <summary>
    /// Creates new COO matrix with the values from the given one.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let copy (clContext: ClContext) workGroupSize =

        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: COO<'a>) ->
            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = copy processor allocationMode matrix.Rows
              Columns = copy processor allocationMode matrix.Columns
              Values = copyData processor allocationMode matrix.Values }

    /// <summary>
    /// Builds a new COO matrix whose elements are the results of applying the given function
    /// to each of the elements of the matrix.
    /// </summary>
    /// <param name="op">
    /// A function to transform values of the input matrix.
    /// Operand and result types should be optional to distinguish explicit and implicit zeroes
    /// </param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let map = Map.run

    /// <summary>
    /// Builds a new COO matrix whose values are the results of applying the given function
    /// to the corresponding pairs of values from the two matrices.
    /// </summary>
    /// <param name="op">
    /// A function to transform pairs of values from the input matrices.
    /// Operands and result types should be optional to distinguish explicit and implicit zeroes
    /// </param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let map2 = Map2.run

    /// <summary>
    /// Builds a new COO matrix whose values are the results of applying the given function
    /// to the corresponding pairs of values from the two matrices.
    /// </summary>
    /// <param name="op">
    /// A function to transform pairs of values from the input matrices.
    /// Operation assumption: one of the operands should always be non-zero.
    /// </param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let rec map2AtLeastOne<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        Map2.AtLeastOne.run clContext (Convert.atLeastOneToOption opAdd) workGroupSize

    /// <summary>
    /// Converts <c>COO</c> matrix format to <c>Tuple</c>.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let getTuples (clContext: ClContext) workGroupSize =

        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.COO<'a>) ->

            let resultRows =
                copy processor allocationMode matrix.Rows

            let resultColumns =
                copy processor allocationMode matrix.Columns

            let resultValues =
                copyData processor allocationMode matrix.Values

            { Context = clContext
              RowIndices = resultRows
              ColumnIndices = resultColumns
              Values = resultValues }

    /// <summary>
    /// Converts rows of given COO matrix to rows in CSR format of the same matrix.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let compressRows (clContext: ClContext) workGroupSize =

        let compressRows =
            <@ fun (ndRange: Range1D) (rows: ClArray<int>) (nnz: int) (rowPointers: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if i < nnz then
                    let row = rows.[i]

                    if i = 0 || row <> rows.[i - 1] then
                        rowPointers.[row] <- i @>

        let program = clContext.Compile(compressRows)

        let create = ClArray.create clContext workGroupSize

        let scan =
            Common.PrefixSum.runBackwardsIncludeInPlace <@ min @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (rowIndices: ClArray<int>) rowCount ->

            let nnz = rowIndices.Length

            let rowPointers =
                create processor allocationMode (rowCount + 1) nnz

            let kernel = program.GetKernel()

            let ndRange = Range1D.CreateValid(nnz, workGroupSize)
            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange rowIndices nnz rowPointers))
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            (scan processor rowPointers nnz).Free processor

            rowPointers

    /// <summary>
    /// Converts the given COO matrix to CSR format.
    /// Values and columns are copied and do not depend on input COO matrix anymore.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSR (clContext: ClContext) workGroupSize =
        let prepare = compressRows clContext workGroupSize

        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.COO<'a>) ->
            let rowPointers =
                prepare processor allocationMode matrix.Rows matrix.RowCount

            let cols =
                copy processor allocationMode matrix.Columns

            let values =
                copyData processor allocationMode matrix.Values

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              RowPointers = rowPointers
              Columns = cols
              Values = values }

    /// <summary>
    /// Converts the given COO matrix to CSR format.
    /// Values and columns are NOT copied and still depend on the input COO matrix.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSRInPlace (clContext: ClContext) workGroupSize =
        let prepare = compressRows clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.COO<'a>) ->
            let rowPointers =
                prepare processor allocationMode matrix.Rows matrix.RowCount

            matrix.Rows.Free processor

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              RowPointers = rowPointers
              Columns = matrix.Columns
              Values = matrix.Values }

    /// <summary>
    /// Transposes the given matrix and returns result.
    /// The given matrix should neither be used afterwards nor be disposed.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let transposeInPlace (clContext: ClContext) workGroupSize =

        let sort =
            Common.Sort.Bitonic.sortKeyValuesInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) (matrix: ClMatrix.COO<'a>) ->
            sort queue matrix.Columns matrix.Rows matrix.Values

            { Context = clContext
              RowCount = matrix.ColumnCount
              ColumnCount = matrix.RowCount
              Rows = matrix.Columns
              Columns = matrix.Rows
              Values = matrix.Values }

    /// <summary>
    /// Transposes the given matrix and returns result as a new matrix.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let transpose (clContext: ClContext) workGroupSize =

        let transposeInPlace = transposeInPlace clContext workGroupSize

        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.COO<'a>) ->

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = copy queue allocationMode matrix.Rows
              Columns = copy queue allocationMode matrix.Columns
              Values = copyData queue allocationMode matrix.Values }
            |> transposeInPlace queue

    /// <summary>
    /// Builds a bitmap. Maps non-zero elements of the left matrix
    /// to 1 if the right matrix has non zero element under the same row and column pair, otherwise 0.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let findKeysIntersection (clContext: ClContext) workGroupSize =
        Intersect.findKeysIntersection clContext workGroupSize

    /// <summary>
    /// Merges two disjoint matrices of the same size.
    /// </summary>
    /// <remarks>
    /// Matrices should have the same number of rows and columns. <br/>
    /// Matrices should not have non zero values with the same index.
    /// </remarks>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let mergeDisjoint (clContext: ClContext) workGroupSize =
        Merge.runDisjoint clContext workGroupSize

    let ofList (clContext: ClContext) allocationMode rowCount columnCount (elements: (int * int * 'a) list) =
        let rows, columns, values =
            let elements = elements |> Array.ofList

            elements
            |> Array.sortInPlaceBy (fun (x, _, _) -> x)

            elements |> Array.unzip3

        { Context = clContext
          Rows = clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, rows)
          Columns = clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, columns)
          Values = clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, values)
          RowCount = rowCount
          ColumnCount = columnCount }

    /// <summary>
    /// Returns matrix composed of all elements from the given row range of the input matrix.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let subRows (clContext: ClContext) workGroupSize =

        let upperBound =
            ClArray.upperBound clContext workGroupSize

        let blit = ClArray.blit clContext workGroupSize

        let blitData = ClArray.blit clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode startRow count (matrix: ClMatrix.COO<'a>) ->
            if count <= 0 then
                failwith "Count must be greater than zero"

            if startRow < 0 then
                failwith "startIndex must be greater then zero"

            if startRow + count > matrix.RowCount then
                failwith "startIndex and count sum is larger than the matrix row count"

            let firstRowClCell = clContext.CreateClCell(startRow - 1)
            let lastRowClCell = clContext.CreateClCell(startRow + count)

            // extract rows
            let firstIndex =
                (upperBound processor matrix.Rows firstRowClCell)
                    .ToHostAndFree processor

            let lastIndex =
                (upperBound processor matrix.Rows lastRowClCell)
                    .ToHostAndFree processor
                - 1

            firstRowClCell.Free processor
            lastRowClCell.Free processor

            let resultLength = lastIndex - firstIndex + 1

            let rows =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

            blit processor matrix.Columns firstIndex rows 0 resultLength

            // extract values
            let values =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

            blitData processor matrix.Values firstIndex values 0 resultLength

            // extract indices
            let columns =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

            blit processor matrix.Columns firstIndex columns 0 resultLength

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = rows
              Columns = columns
              Values = values }
