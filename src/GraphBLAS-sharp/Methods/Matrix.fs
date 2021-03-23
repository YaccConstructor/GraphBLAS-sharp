namespace GraphBLAS.FSharp

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open GraphBLAS.FSharp.Backend

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

    let tuples (matrix: Matrix<'a>) : GraphblasEvaluation<MatrixTuples<'a>> =
        let matrixTuples =
            match matrix with
            | MatrixCOO coo -> COOMatrix.GetTuples.from coo
            | _ -> failwith "Not Implemented"

        graphblas { return! EvalGB.fromCl matrixTuples }

    let mask (matrix: Matrix<'a>) : GraphblasEvaluation<Mask2D> = failwith "Not Implemented yet"
    let complemented (matrix: Matrix<'a>) : GraphblasEvaluation<Mask2D> = failwith "Not Implemented yet"
    let synchronize (matrix: Matrix<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"

    (*
        assignment, extraction and filling
    *)

    /// mat.[mask]
    let extractSubMatrix (mask: Mask2D option) (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> =
        failwith "Not Implemented yet"

    /// mat.[rowIdx. *]
    let extractRow (rowIdx: int) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    /// mat.[rowIdx, mask]
    let extractSubRow (rowIdx: int) (mask: Mask2D) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    /// mat.[*, colIdx]
    let extractCol (colIdx: int) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    /// mat.[mask. colIdx]
    let extractSubCol (colIdx: int) (mask: Mask2D) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    /// mat.[rowIdx, colIdx]
    let extractValue (rowIdx: int) (colIdx: int) (matrix: Matrix<'a>) : GraphblasEvaluation<Scalar<'a>> =
        failwith "Not Implemented yet"

    /// t <- s
    let assignMatrix (source: Matrix<'a>) (target: Matrix<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// t.[mask] <- s
    let assignSubMatrix (mask: Mask2D) (source: Matrix<'a>) (target: Matrix<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// t.[rowIdx, *] <- s
    let assignRow (rowIdx: int) (source: Vector<'a>) (target: Matrix<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// t.[rowIdx, mask] <- s
    let assignSubRow (rowIdx: int) (mask: Mask1D) (source: Vector<'a>) (target: Matrix<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// t.[*, colIdx] <- s
    let assignCol (colIdx: int) (source: Vector<'a>) (target: Matrix<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// t.[mask, colIdx] <- s
    let assignSubCol (colIdx: int) (mask: Mask1D) (source: Vector<'a>) (target: Matrix<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// mat.[i, j] <- value
    let assignValue (rowIdx: int) (colIdx: int) (value: Scalar<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// mat.[*, *] <- value
    let fillMatrix (value: Scalar<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// mat.[mask] <- value
    let fillSubMatrix (mask: Mask2D) (value: Scalar<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// mat.[rowIdx, *] <- value
    let fillRow (rowIdx: int) (value: Scalar<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// mat.[rowIdx, mask] <- value
    let fillSubRow (rowIdx: int) (mask: Mask1D) (value: Scalar<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// mat.[*, colIdx] <- value
    let fillCol (colIdx: int) (value: Scalar<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// mat.[mask, colIdx] <- value
    let fillSubCol (colIdx: int) (mask: Mask1D) (value: Scalar<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    (*
        operations
    *)

    let mxm (semiring: ISemiring<'a>) (leftMatrix: Matrix<'a>) (rightMatrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> = failwith "Not Implemented yet"
    let mxv (semiring: ISemiring<'a>) (matrix: Matrix<'a>) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"

    let eWiseAdd (semiring: ISemiring<'a>) (leftMatrix: Matrix<'a>) (rightMatrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> =
        let operationResult =
            match leftMatrix, rightMatrix with
            | MatrixCOO left, MatrixCOO right ->
                opencl {
                    let! result = COOMatrix.EWiseAdd.run left right None semiring
                    return MatrixCOO result
                }
            | _ -> failwith "Not Implemented"

        graphblas { return! EvalGB.fromCl operationResult }

    let eWiseMult (semiring: ISemiring<'a>) (leftMatrix: Matrix<'a>) (rightMatrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> = failwith "Not Implemented yet"
    let apply (mapper: UnaryOp<'a, 'b>) (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'b>> = failwith "Not Implemented yet"
    let prune (predicate: UnaryOp<'a, bool>) (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> = failwith "Not Implemented yet"
    let reduceRows (monoid: IMonoid<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let reduceCols (monoid: IMonoid<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let reduce (monoid: IMonoid<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<Scalar<'a>> = failwith "Not Implemented yet"
    let transpose (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'b>> = failwith "Not Implemented yet"
    let kronecker (semiring: ISemiring<'a>) (leftMatrix: Matrix<'a>) (rightMatrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> = failwith "Not Implemented yet"

    let mxmWithMask (semiring: ISemiring<'a>) (mask: Mask2D) (leftMatrix: Matrix<'a>) (rightMatrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> = failwith "Not Implemented yet"
    let mxvWithMask (semiring: ISemiring<'a>) (mask: Mask1D) (matrix: Matrix<'a>) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let eWiseAddWithMask (semiring: ISemiring<'a>) (mask: Mask2D) (leftMatrix: Matrix<'a>) (rightMatrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> = failwith "Not Implemented yet"
    let eWiseMultWithMask (semiring: ISemiring<'a>) (mask: Mask2D) (leftMatrix: Matrix<'a>) (rightMatrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> = failwith "Not Implemented yet"
    let applyWithMask (mapper: UnaryOp<'a, 'b>) (mask: Mask2D) (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'b>> = failwith "Not Implemented yet"
    let pruneWithMask (predicate: UnaryOp<'a, bool>) (mask: Mask2D) (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> = failwith "Not Implemented yet"
    let reduceRowsWithMask (monoid: IMonoid<'a>) (mask: Mask1D) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let reduceColsWithMask (monoid: IMonoid<'a>) (mask: Mask1D) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let kroneckerWithMask (semiring: ISemiring<'a>) (mask: Mask2D) (leftMatrix: Matrix<'a>) (rightMatrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> = failwith "Not Implemented yet"
