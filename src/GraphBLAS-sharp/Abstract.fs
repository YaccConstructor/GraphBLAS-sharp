namespace GraphBLAS.FSharp

open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic

// algebraic objects

type MatrixTuples<'a when 'a : struct and 'a : equality> =
    {
        RowIndices: int[]
        ColumnIndices: int[]
        Values: 'a[]
    }

    member this.ToHost() =
        opencl {
            let! rows = ToHost this.RowIndices
            let! cols = ToHost this.ColumnIndices
            let! vals = ToHost this.Values

            return {
                RowIndices = rows
                ColumnIndices = cols
                Values = vals
            }
        }

type Scalar<'a when 'a : struct and 'a : equality> = Scalar of 'a
with
    static member op_Implicit (Scalar source) = source

[<AbstractClass>]
type Matrix<'a when 'a : struct and 'a : equality>(nrow: int, ncol: int) =
    abstract RowCount: int
    abstract ColumnCount: int
    default this.RowCount = nrow
    default this.ColumnCount = ncol

    abstract Clear: unit -> OpenCLEvaluation<unit>
    abstract Copy: unit -> OpenCLEvaluation<Matrix<'a>>
    abstract Resize: int -> int -> OpenCLEvaluation<Matrix<'a>>
    abstract GetNNZ: unit -> OpenCLEvaluation<int>
    abstract GetTuples: unit -> OpenCLEvaluation<MatrixTuples<'a>>
    abstract GetMask: ?isComplemented: bool -> OpenCLEvaluation<Mask2D option>
    abstract ToHost: unit -> OpenCLEvaluation<Matrix<'a>>

    abstract Extract: Mask2D option -> OpenCLEvaluation<Matrix<'a>>
    abstract Extract: (Mask1D option * int) -> OpenCLEvaluation<Vector<'a>>
    abstract Extract: (int * Mask1D option) -> OpenCLEvaluation<Vector<'a>>
    abstract Extract: (int * int) -> OpenCLEvaluation<Scalar<'a>>
    abstract Assign: Mask2D option * Matrix<'a> -> OpenCLEvaluation<unit>
    abstract Assign: (Mask1D option * int) * Vector<'a> -> OpenCLEvaluation<unit>
    abstract Assign: (int * Mask1D option) * Vector<'a> -> OpenCLEvaluation<unit>
    abstract Assign: (int * int) * Scalar<'a> -> OpenCLEvaluation<unit>
    abstract Assign: Mask2D option * Scalar<'a> -> OpenCLEvaluation<unit>
    abstract Assign: (Mask1D option * int) * Scalar<'a> -> OpenCLEvaluation<unit>
    abstract Assign: (int * Mask1D option) * Scalar<'a> -> OpenCLEvaluation<unit>

    abstract Mxm: Matrix<'a> -> Mask2D option -> ISemiring<'a> -> OpenCLEvaluation<Matrix<'a>>
    abstract Mxv: Vector<'a> -> Mask1D option -> ISemiring<'a> -> OpenCLEvaluation<Vector<'a>>
    abstract EWiseAdd: Matrix<'a> -> Mask2D option -> ISemiring<'a> -> OpenCLEvaluation<Matrix<'a>>
    abstract EWiseMult: Matrix<'a> -> Mask2D option -> ISemiring<'a> -> OpenCLEvaluation<Matrix<'a>>
    abstract Apply: Mask2D option -> UnaryOp<'a, 'b> -> OpenCLEvaluation<Matrix<'b>>
    abstract Prune: Mask2D option -> UnaryOp<'a, bool> -> OpenCLEvaluation<Matrix<'a>>
    abstract ReduceIn: Mask1D option -> IMonoid<'a> -> OpenCLEvaluation<Vector<'a>>
    abstract ReduceOut: Mask1D option -> IMonoid<'a> -> OpenCLEvaluation<Vector<'a>>
    abstract Reduce: IMonoid<'a> -> OpenCLEvaluation<Scalar<'a>>
    abstract Transpose: unit -> OpenCLEvaluation<Matrix<'a>>
    abstract Kronecker: Matrix<'a> -> Mask2D option -> ISemiring<'a> -> OpenCLEvaluation<Matrix<'a>>

and [<AbstractClass>] Vector<'a when 'a : struct and 'a : equality>(size: int) =
    abstract Size: int
    default this.Size = size

    abstract Clear: unit -> OpenCLEvaluation<unit>
    abstract Copy: unit -> OpenCLEvaluation<Vector<'a>>
    abstract Resize: int -> OpenCLEvaluation<Vector<'a>>
    abstract GetNNZ: unit -> OpenCLEvaluation<int>
    abstract GetTuples: unit -> OpenCLEvaluation<{| Indices: int[]; Values: 'a[] |}>
    abstract GetMask: ?isComplemented: bool -> OpenCLEvaluation<Mask1D option>
    abstract ToHost: unit -> OpenCLEvaluation<Vector<'a>>

    abstract Extract: Mask1D option -> OpenCLEvaluation<Vector<'a>>
    abstract Extract: int -> OpenCLEvaluation<Scalar<'a>>
    abstract Assign: Mask1D option * Vector<'a> -> OpenCLEvaluation<unit>
    abstract Assign: int * Scalar<'a> -> OpenCLEvaluation<unit>
    abstract Assign: Mask1D option * Scalar<'a> -> OpenCLEvaluation<unit>

    abstract Vxm: Matrix<'a> -> Mask1D option -> ISemiring<'a> -> OpenCLEvaluation<Vector<'a>>
    abstract EWiseAdd: Vector<'a> -> Mask1D option -> ISemiring<'a> -> OpenCLEvaluation<Vector<'a>>
    abstract EWiseMult: Vector<'a> -> Mask1D option -> ISemiring<'a> -> OpenCLEvaluation<Vector<'a>>
    abstract Apply: Mask1D option -> UnaryOp<'a, 'b> -> OpenCLEvaluation<Vector<'b>>
    abstract Prune: Mask1D option -> UnaryOp<'a, bool> -> OpenCLEvaluation<Vector<'a>>
    abstract Reduce: IMonoid<'a> -> OpenCLEvaluation<Scalar<'a>>

and Mask1D(indices: int[], size: int, isComplemented: bool) =
    member this.Indices = indices
    member this.Size = size
    member this.IsComplemented = isComplemented

and Mask2D(rowIndices: int[], columnIndices: int[], rowCount: int, columnCount: int, isComplemented: bool) =
    member this.RowIndices = rowIndices
    member this.ColumnIndices = columnIndices
    member this.RowCount = rowCount
    member this.ColumnCount = columnCount
    member this.IsComplemented = isComplemented

type COOFormat<'a> =
    {
        RowCount: int
        ColumnCount: int
        Rows: int[]
        Columns: int[]
        Values: 'a[]
    }

type CSRFormat<'a> =
    {
        ColumnCount: int
        RowPointers: int[]
        ColumnIndices: int[]
        Values: 'a[]
    }

    static member CreateEmpty<'a>() = {
        RowPointers = Array.zeroCreate<int> 0
        ColumnIndices = Array.zeroCreate<int> 0
        Values = Array.zeroCreate<'a> 0
        ColumnCount = 0
    }
