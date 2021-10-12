namespace GraphBLAS.FSharp

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open GraphBLAS.FSharp.Backend

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Matrix =

    (*
        constructors
    *)

    let build
        (rowCount: int)
        (columnCount: int)
        (rows: int [])
        (columns: int [])
        (values: 'a [])
        : GraphblasEvaluation<Matrix<'a>> =
        failwith "Not Implemented yet"

    let ofTuples (rowCount: int) (columnCount: int) (tuples: MatrixTuples<'a>) : GraphblasEvaluation<Matrix<'a>> =
        failwith "Not Implemented yet"

    let ofList (rowCount: int) (columnCount: int) (elements: (int * int * 'a) list) : GraphblasEvaluation<Matrix<'a>> =
        failwith "Not Implemented yet"

    // можно оставить, но с условием, что будет создаваться full matrix,
    // которую можно будет проредить потом (но вообще это initом эмулируется)
    // let ofArray2D (array: 'a[,]) : GraphblasEvaluation<Matrix<'a>> =
    //     failwith "Not Implemented yet""

    let init (rowCount: int) (columnCount: int) (initializer: int -> int -> 'a) : GraphblasEvaluation<Matrix<'a>> =
        failwith "Not Implemented yet"

    let create (rowCount: int) (columnCount: int) (value: 'a) : GraphblasEvaluation<Matrix<'a>> =
        failwith "Not Implemented yet"

    let zeroCreate<'a when 'a: struct> (rowCount: int) (columnCount: int) : GraphblasEvaluation<Matrix<'a>> =
        failwith "Not Implemented yet"

    (*
        methods
    *)

    let rowCount (matrix: Matrix<'a>) : int = matrix.RowCount
    let columnCount (matrix: Matrix<'a>) : int = matrix.ColumnCount

    let copy (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> = failwith "Not Implemented yet"

    let resize (rowCount: int) (columnCount: int) (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> =
        failwith "Not Implemented yet"

    // NOTE int cant be sync
    let nnz (matrix: Matrix<'a>) : GraphblasEvaluation<int> = failwith "Not Implemented yet"

    let tuples (matrix: Matrix<'a>) : GraphblasEvaluation<MatrixTuples<'a>> =
        match matrix with
        | MatrixCOO matrix -> COOMatrix.GetTuples.fromMatrix matrix
        | MatrixCSR matrix -> CSRMatrix.GetTuples.fromMatrix matrix
        |> EvalGB.fromCl

    let mask (matrix: Matrix<'a>) : GraphblasEvaluation<Mask2D> = failwith "Not Implemented yet"
    let complemented (matrix: Matrix<'a>) : GraphblasEvaluation<Mask2D> = failwith "Not Implemented yet"

    let switch (matrixFormat: MatrixFromat) (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> =
        match matrix, matrixFormat with
        | MatrixCOO matrix, CSR ->
            opencl {
                let! result = CSRMatrix.Convert.fromCoo matrix
                return MatrixCSR result
            }
        | _ -> failwith "Not Implemented"
        |> EvalGB.fromCl

    let synchronize (matrix: Matrix<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"

    let synchronizeAndReturn (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> =
        match matrix with
        | MatrixCSR matrix ->
            opencl {
                let! _ =
                    if matrix.RowPointers.Length = 0 then
                        opencl { return [||] }
                    else
                        ToHost matrix.RowPointers

                let! _ =
                    if matrix.ColumnIndices.Length = 0 then
                        opencl { return [||] }
                    else
                        ToHost matrix.ColumnIndices

                let! _ =
                    if matrix.Values.Length = 0 then
                        opencl { return [||] }
                    else
                        ToHost matrix.Values

                return MatrixCSR matrix
            }
        | _ -> failwith "Not Implemented"
        |> EvalGB.fromCl

    (*
        assignment, extraction and filling
    *)

    /// mat.[mask]
    let extractSubMatrix (matrix: Matrix<'a>) (mask: Mask2D) : GraphblasEvaluation<Matrix<'a>> =
        failwith "Not Implemented yet"

    /// mat.[rowIdx. *]
    let extractRow (matrix: Matrix<'a>) (rowIdx: int) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"

    /// mat.[rowIdx, mask]
    let extractSubRow (matrix: Matrix<'a>) (rowIdx: int) (mask: Mask2D) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    /// mat.[*, colIdx]
    let extractCol (matrix: Matrix<'a>) (colIdx: int) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"

    /// mat.[mask. colIdx]
    let extractSubCol (matrix: Matrix<'a>) (mask: Mask2D) (colIdx: int) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    /// mat.[rowIdx, colIdx]
    let extractValue (matrix: Matrix<'a>) (rowIdx: int) (colIdx: int) : GraphblasEvaluation<Scalar<'a>> =
        failwith "Not Implemented yet"

    /// t <- s
    let assignMatrix (target: Matrix<'a>) (source: Matrix<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// t.[mask] <- s
    let assignSubMatrix (target: Matrix<'a>) (mask: Mask2D) (source: Matrix<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// t.[rowIdx, *] <- s
    let assignRow (target: Matrix<'a>) (rowIdx: int) (source: Vector<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// t.[rowIdx, mask] <- s
    let assignSubRow
        (target: Matrix<'a>)
        (rowIdx: int)
        (mask: Mask1D)
        (source: Vector<'a>)
        : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// t.[*, colIdx] <- s
    let assignCol (target: Matrix<'a>) (colIdx: int) (source: Vector<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// t.[mask, colIdx] <- s
    let assignSubCol
        (target: Matrix<'a>)
        (colIdx: int)
        (mask: Mask1D)
        (source: Vector<'a>)
        : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// mat.[*, *] <- value
    let fillMatrix (value: Scalar<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"

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
        closed unmasked operations
    *)

    let mxm
        (semiring: ISemiring<'a>)
        (leftMatrix: Matrix<'a>)
        (rightMatrix: Matrix<'a>)
        : GraphblasEvaluation<Matrix<'a>> =
        failwith "Not Implemented"

    let mxv (semiring: ISemiring<'a>) (matrix: Matrix<'a>) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> =
        match matrix, vector with
        | MatrixCSR matrix, VectorCOO vector ->
            opencl {
                let! result = CSRMatrix.SpMSpV.unmasked matrix vector semiring
                return VectorCOO result
            }
        | _ -> failwith "Not Implemented"
        |> EvalGB.fromCl

    let vxm (semiring: ISemiring<'a>) (vector: Vector<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented"

    let eWiseAdd
        (monoid: IMonoid<'a>)
        (leftMatrix: Matrix<'a>)
        (rightMatrix: Matrix<'a>)
        : GraphblasEvaluation<Matrix<'a>> =
        match leftMatrix, rightMatrix with
        | MatrixCOO left, MatrixCOO right ->
            opencl {
                let! result = COOMatrix.EWiseAdd.run left right None monoid
                return MatrixCOO result
            }
        | _ -> failwith "Not Implemented"
        |> EvalGB.fromCl

    let eWiseMult
        (semiring: ISemiring<'a>)
        (leftMatrix: Matrix<'a>)
        (rightMatrix: Matrix<'a>)
        : GraphblasEvaluation<Matrix<'a>> =
        failwith "Not Implemented yet"

    let apply (mapper: UnaryOp<'a, 'b>) (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'b>> =
        failwith "Not Implemented yet"

    let select (predicate: UnaryOp<int * int * 'a, bool>) (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> =
        failwith "Not Implemented yet"

    let reduceRows (monoid: IMonoid<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let reduceCols (monoid: IMonoid<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let reduce (monoid: IMonoid<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<Scalar<'a>> =
        failwith "Not Implemented yet"

    let transpose (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'a>> =
        match matrix with
        | MatrixCSR matrix ->
            // map
            opencl {
                let! transposed = CSRMatrix.Transpose.transposeMatrix matrix
                return MatrixCSR transposed
            }
        | _ -> failwith "Not Implemented"
        |> EvalGB.fromCl

    let kronecker
        (semiring: ISemiring<'a>)
        (leftMatrix: Matrix<'a>)
        (rightMatrix: Matrix<'a>)
        : GraphblasEvaluation<Matrix<'a>> =
        failwith "Not Implemented yet"

    (*
        closed masked operations
    *)

    let mxmWithMask
        (semiring: ISemiring<'a>)
        (mask: Mask2D)
        (leftMatrix: Matrix<'a>)
        (rightMatrix: Matrix<'a>)
        : GraphblasEvaluation<Matrix<'a>> =
        failwith "Not Implemented yet"

    let mxvWithMask
        (semiring: ISemiring<'a>)
        (mask: Mask1D)
        (matrix: Matrix<'a>)
        (vector: Vector<'a>)
        : GraphblasEvaluation<Vector<'a>> =
        match matrix, vector, mask with
        | MatrixCSR matrix, VectorCOO vector, mask when not mask.IsComplemented ->
            opencl {
                let! result = CSRMatrix.SpMSpV.masked matrix vector semiring mask
                return VectorCOO result
            }
        | _ -> failwith "Not Implemented"
        |> EvalGB.fromCl

    let vxmWithMask
        (semiring: ISemiring<'a>)
        (mask: Mask1D)
        (vector: Vector<'a>)
        (matrix: Matrix<'a>)
        : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let eWiseAddWithMask
        (monoid: IMonoid<'a>)
        (mask: Mask2D)
        (leftMatrix: Matrix<'a>)
        (rightMatrix: Matrix<'a>)
        : GraphblasEvaluation<Matrix<'a>> =
        failwith "Not Implemented yet"

    let eWiseMultWithMask
        (semiring: ISemiring<'a>)
        (mask: Mask2D)
        (leftMatrix: Matrix<'a>)
        (rightMatrix: Matrix<'a>)
        : GraphblasEvaluation<Matrix<'a>> =
        failwith "Not Implemented yet"

    let applyWithMask (mapper: UnaryOp<'a, 'b>) (mask: Mask2D) (matrix: Matrix<'a>) : GraphblasEvaluation<Matrix<'b>> =
        failwith "Not Implemented yet"

    let selectWithMask
        (predicate: UnaryOp<int * int * 'a, bool>)
        (mask: Mask2D)
        (matrix: Matrix<'a>)
        : GraphblasEvaluation<Matrix<'a>> =
        failwith "Not Implemented yet"

    let reduceRowsWithMask (monoid: IMonoid<'a>) (mask: Mask1D) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let reduceColsWithMask (monoid: IMonoid<'a>) (mask: Mask1D) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let kroneckerWithMask
        (semiring: ISemiring<'a>)
        (mask: Mask2D)
        (leftMatrix: Matrix<'a>)
        (rightMatrix: Matrix<'a>)
        : GraphblasEvaluation<Matrix<'a>> =
        failwith "Not Implemented yet"

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module MatrixTuples =
    let synchronize (matrixTuples: MatrixTuples<'a>) =
        opencl {
            let! _ =
                if matrixTuples.RowIndices.Length = 0 then
                    opencl { return [||] }
                else
                    ToHost matrixTuples.RowIndices

            let! _ =
                if matrixTuples.ColumnIndices.Length = 0 then
                    opencl { return [||] }
                else
                    ToHost matrixTuples.ColumnIndices

            let! _ =
                if matrixTuples.Values.Length = 0 then
                    opencl { return [||] }
                else
                    ToHost matrixTuples.Values

            return ()
        }
        |> EvalGB.fromCl
