namespace rec GraphBLAS.FSharp.Backend.CSRMatrix

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common
open Brahma.OpenCL

type internal Transpose<'a when 'a : struct>(matrix: CSRMatrix<'a>) =
    member this.Invoke() = opencl {
        // let! coo = Transpose.csr2coo matrix
        return matrix
    }

module Transpose =
    let csr2coo<'a when 'a : struct> (matrix: CSRMatrix<'a>) = opencl {
        let wgSize = Utils.defaultWorkGroupSize
        let expandRows =
            <@
                fun (range: _1D)
                    (rowPointers: int[])
                    (rowIndices: int[]) ->

                    let lid = range.LocalID0
                    let groupId = range.GlobalID0 / wgSize

                    let rowStart = rowPointers.[groupId]
                    let rowEnd = rowPointers.[groupId + 1]
                    let rowLength = rowEnd - rowStart

                    for i in lid .. wgSize .. rowLength - 1 do
                        rowIndices.[rowStart + i] <- groupId
            @>

        let rowIndices = Array.zeroCreate<int> matrix.Values.Length
        do! RunCommand expandRows <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(wgSize * matrix.RowCount, wgSize)
            <| matrix.RowPointers
            <| rowIndices

        let! colIndices = Copy.copyArray matrix.ColumnIndices
        let! values = Copy.copyArray matrix.Values

        return {
            RowCount = matrix.RowCount
            ColumnCount = matrix.ColumnCount
            Rows = rowIndices
            Columns = colIndices
            Values = values
        }
    }
