namespace GraphBLAS.FSharp

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp.Backend

type MatrixTuples<'a> =
    {
        RowIndices: int[]
        ColumnIndices: int[]
        Values: 'a[]
    }

// ждём тайпклассов чтобы можно было вызывать synchronize для всех объектов,
// для которых он реализован, не привязывая реализацию к классу (как стратегия)
module MatrixTuples =
    let synchronize (matrixTuples: MatrixTuples<'a>) =
        opencl {
            let! _ = ToHost matrixTuples.RowIndices
            let! _ = ToHost matrixTuples.ColumnIndices
            let! _ = ToHost matrixTuples.Values
            return ()
        }
        |> EvalGB.fromCl

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Matrix =

    (*
        constructors
    *)

    let build (rowCount: int) (columnCount: int) (rows: int[]) (columns: int[]) (values: 'a[]) : Matrix<'a> =
        failwith "Not Implemented yet"

    let ofArray2D (isZero: 'a -> bool) (array: 'a[,]) : Matrix<'a> =
        failwith "Not Implemented yet"

    let fromFile (pathToMatrix: string) : Matrix<'a> =
        failwith "Not Implemented yet"

    let init (rowCount: int) (columnCount: int) (initializer: int -> int -> 'a) : Matrix<'a> =
        failwith "Not Implemented yet"

    let zeroCreate (rowCount: int) (columnCount: int) : Matrix<'a> =
        failwith "Not Implemented yet"

    (*
        methods
    *)

    let rowCount (matrix: Matrix<'a>) : int = failwith "Not Implemented yet"
    let columnCount (matrix: Matrix<'a>) : int = failwith "Not Implemented yet"
    let clear (matrix: Matrix<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"
    let copy (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> = failwith "Not Implemented yet"
    let resize (rowCount: int) (columnCount: int) (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> = failwith "Not Implemented yet"
    let nnz (matrix: Matrix<'a>) : GraphblasEvaluation<int> = failwith "Not Implemented yet"
    let tuples (matrix: Matrix<'a>) : GraphblasEvaluation<MatrixTuples<'a>> = failwith "Not Implemented yet"
    let mask (matrix: Matrix<'a>) : GraphblasEvaluation<Mask2D option> = failwith "Not Implemented yet"
    let complemented (matrix: Matrix<'a>) : GraphblasEvaluation<Mask2D option> = failwith "Not Implemented yet"
    let synchronize (matrix: Matrix<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"

    (*
        assignment, extraction and filling
    *)

    let extractSubMatrix (mask: Mask2D option) (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> = failwith "Not Implemented yet"
    let extractSubRow (rowIdx: int) (mask: Mask2D option) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let extractSubCol (colIdx: int) (mask: Mask2D option) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let extractElement (rowIdx: int) (colIdx: int) (matrix: Matrix<'a>) : GraphblasEvaluation<Scalar<'a>> = failwith "Not Implemented yet"
    let assignSubMatrix (mask: Mask2D option) (source: Matrix<'a>) (target: Matrix<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"
    let assignSubRow (rowIdx: int) (mask: Mask2D option) (source: Vector<'a>) (target: Matrix<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"
    let assignSubCol (colIdx: int) (mask: Mask2D option) (source: Vector<'a>) (target: Matrix<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"
    let assignElement (rowIdx: int) (colIdx: int) (source: Scalar<'a>) (target: Matrix<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"
    let fillSubMatrix (mask: Mask2D option) (filler: Scalar<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"
    let fillSubRow (rowIdx: int) (mask: Mask2D option) (filler: Scalar<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"
    let fillSubCol (colIdx: int) (mask: Mask2D option) (filler: Scalar<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"

    (*
        operations
    *)

    let mxm (semiring: ISemiring<'a>) (mask: Mask2D option) (leftMatrix: Matrix<'a>) (rightMatrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> = failwith "Not Implemented yet"
    let mxv (semiring: ISemiring<'a>) (mask: Mask1D option) (matrix: Matrix<'a>) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"

    let eWiseAdd (semiring: ISemiring<'a>) (mask: Mask2D option) (leftMatrix: Matrix<'a>) (rightMatrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> =
        let operationResult =
            match leftMatrix, rightMatrix with
            | MatrixCOO left, MatrixCOO right ->
                opencl {
                    let! result = COOMatrix.EWiseAdd.run left right mask semiring
                    return MatrixCOO result
                }
            | _ -> failwith "Not Implemented"

        graphblas { return! EvalGB.fromCl operationResult }

    let eWiseMult (semiring: ISemiring<'a>) (mask: Mask2D option) (leftMatrix: Matrix<'a>) (rightMatrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> = failwith "Not Implemented yet"
    let apply (mapper: UnaryOp<'a, 'b>) (mask: Mask2D option) (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'b>> = failwith "Not Implemented yet"
    let prune (predicate: UnaryOp<'a, bool>) (mask: Mask2D option) (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> = failwith "Not Implemented yet"
    let reduceRows (monoid: IMonoid<'a>) (mask: Mask1D) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let reduceCols (monoid: IMonoid<'a>) (mask: Mask1D) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let reduce (monoid: IMonoid<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<Scalar<'a>> = failwith "Not Implemented yet"
    let transpose (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'b>> = failwith "Not Implemented yet"
    let kronecker (semiring: ISemiring<'a>) (mask: Mask2D option) (leftMatrix: Matrix<'a>) (rightMatrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> = failwith "Not Implemented yet"
