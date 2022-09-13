namespace GraphBLAS.FSharp

open Brahma.FSharp

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
        : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"

    let ofTuples (rowCount: int) (columnCount: int) (tuples: Backend.MatrixTuples<'a>) : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"

    let ofList (rowCount: int) (columnCount: int) (elements: (int * int * 'a) list) : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"

    // можно оставить, но с условием, что будет создаваться full matrix,
    // которую можно будет проредить потом (но вообще это initом эмулируется)
    // let ofArray2D (array: 'a[,]) : GraphblasEvaluation<Matrix<'a>> =
    //     failwith "Not Implemented yet""

    let init (rowCount: int) (columnCount: int) (initializer: int -> int -> 'a) : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"

    let create (rowCount: int) (columnCount: int) (value: 'a) : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"

    let zeroCreate<'a when 'a: struct> (rowCount: int) (columnCount: int) : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"

    (*
        methods
    *)

    let rowCount (matrix: Mat<'a>) : int = matrix.RowCount
    let columnCount (matrix: Mat<'a>) : int = matrix.ColumnCount

    let copy (matrix: Mat<'a>) : GraphblasEvaluation<Mat<'a>> = failwith "Not Implemented yet"

    let resize (rowCount: int) (columnCount: int) (matrix: Mat<'a>) : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"

    // NOTE int cant be sync
    let nnz (matrix: Mat<'a>) : GraphblasEvaluation<int> = failwith "Not Implemented yet"

    let tuples (matrix: Mat<'a>) : GraphblasEvaluation<Backend.MatrixTuples<'a>> =
        failwith "Not Implemented yet"
//        match matrix with
//        | MatrixCOO matrix -> COOMatrix.GetTuples.fromMatrix matrix
//        | MatrixCSR matrix -> CSRMatrix.GetTuples.fromMatrix matrix
//        |> EvalGB.fromCl

    let mask (matrix: Mat<'a>) : GraphblasEvaluation<Mask2D> = failwith "Not Implemented yet"
    let complemented (matrix: Mat<'a>) : GraphblasEvaluation<Mask2D> = failwith "Not Implemented yet"

    let switch (matrixFormat: Backend.MatrixFormat) (matrix: Mat<'a>) : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"
//        match matrix, matrixFormat with
//        | MatrixCOO matrix, CSR ->
//            opencl {
//                let! result = CSRMatrix.Convert.fromCoo matrix
//                return MatrixCSR result
//            }
//        | _ -> failwith "Not Implemented"
//        |> EvalGB.fromCl

    let synchronize (matrix: Mat<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"

    let synchronizeAndReturn (matrix: Mat<'a>) : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"
//        match matrix with
//        | MatrixCSR matrix ->
//            opencl {
//                let! _ =
//                    if matrix.RowPointers.Length = 0 then
//                        opencl { return [||] }
//                    else
//                        failwith "FIX ME! And rewrite."
//                //ToHost matrix.RowPointers
//
//                let! _ =
//                    if matrix.ColumnIndices.Length = 0 then
//                        opencl { return [||] }
//                    else
//                        failwith "FIX ME! And rewrite."
//                //ToHost matrix.ColumnIndices
//
//                let! _ =
//                    if matrix.Values.Length = 0 then
//                        opencl { return [||] }
//                    else
//                        failwith "FIX ME! And rewrite."
//                //ToHost matrix.Values
//
//                return MatrixCSR matrix
//            }
//        | _ -> failwith "Not Implemented"
//        |> EvalGB.fromCl

    (*
        assignment, extraction and filling
    *)

    /// mat.[mask]
    let extractSubMatrix (matrix: Mat<'a>) (mask: Mask2D) : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"

    /// mat.[rowIdx. *]
    let extractRow (matrix: Mat<'a>) (rowIdx: int) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"

    /// mat.[rowIdx, mask]
    let extractSubRow (matrix: Mat<'a>) (rowIdx: int) (mask: Mask2D) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    /// mat.[*, colIdx]
    let extractCol (matrix: Mat<'a>) (colIdx: int) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"

    /// mat.[mask. colIdx]
    let extractSubCol (matrix: Mat<'a>) (mask: Mask2D) (colIdx: int) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    /// mat.[rowIdx, colIdx]
    let extractValue (matrix: Mat<'a>) (rowIdx: int) (colIdx: int) : GraphblasEvaluation<Scalar<'a>> =
        failwith "Not Implemented yet"

    /// t <- s
    let assignMatrix (target: Mat<'a>) (source: Mat<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// t.[mask] <- s
    let assignSubMatrix (target: Mat<'a>) (mask: Mask2D) (source: Mat<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// t.[rowIdx, *] <- s
    let assignRow (target: Mat<'a>) (rowIdx: int) (source: Vector<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// t.[rowIdx, mask] <- s
    let assignSubRow
        (target: Mat<'a>)
        (rowIdx: int)
        (mask: Mask1D)
        (source: Vector<'a>)
        : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// t.[*, colIdx] <- s
    let assignCol (target: Mat<'a>) (colIdx: int) (source: Vector<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// t.[mask, colIdx] <- s
    let assignSubCol
        (target: Mat<'a>)
        (colIdx: int)
        (mask: Mask1D)
        (source: Vector<'a>)
        : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// mat.[*, *] <- value
    let fillMatrix (value: Scalar<'a>) (matrix: Mat<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"

    /// mat.[mask] <- value
    let fillSubMatrix (mask: Mask2D) (value: Scalar<'a>) (matrix: Mat<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// mat.[rowIdx, *] <- value
    let fillRow (rowIdx: int) (value: Scalar<'a>) (matrix: Mat<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// mat.[rowIdx, mask] <- value
    let fillSubRow (rowIdx: int) (mask: Mask1D) (value: Scalar<'a>) (matrix: Mat<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// mat.[*, colIdx] <- value
    let fillCol (colIdx: int) (value: Scalar<'a>) (matrix: Mat<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// mat.[mask, colIdx] <- value
    let fillSubCol (colIdx: int) (mask: Mask1D) (value: Scalar<'a>) (matrix: Mat<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    (*
        closed unmasked operations
    *)

    let mxm
        (semiring: ISemiring<'a>)
        (leftMatrix: Mat<'a>)
        (rightMatrix: Mat<'a>)
        : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented"

    let mxv (semiring: ISemiring<'a>) (matrix: Mat<'a>) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"
//        match matrix, vector with
//        | MatrixCSR matrix, VectorCOO vector ->
//            opencl {
//                let! result = CSRMatrix.SpMSpV.unmasked matrix vector semiring
//                return VectorCOO result
//            }
//        | _ -> failwith "Not Implemented"
//        |> EvalGB.fromCl

    let vxm (semiring: ISemiring<'a>) (vector: Vector<'a>) (matrix: Mat<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented"

    let eWiseAdd
        (monoid: IMonoid<'a>)
        (leftMatrix: Mat<'a>)
        (rightMatrix: Mat<'a>)
        : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"
//        match leftMatrix, rightMatrix with
//        | MatrixCOO left, MatrixCOO right ->
//            opencl {
//                let! result = COOMatrix.EWiseAdd.run left right None monoid
//                return MatrixCOO result
//            }
//        | _ -> failwith "Not Implemented"
//        |> EvalGB.fromCl

    let eWiseMult
        (semiring: ISemiring<'a>)
        (leftMatrix: Mat<'a>)
        (rightMatrix: Mat<'a>)
        : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"

    let apply (mapper: UnaryOp<'a, 'b>) (matrix: Mat<'a>) : GraphblasEvaluation<Mat<'b>> =
        failwith "Not Implemented yet"

    let select (predicate: UnaryOp<int * int * 'a, bool>) (matrix: Mat<'a>) : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"

    let reduceRows (monoid: IMonoid<'a>) (matrix: Mat<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let reduceCols (monoid: IMonoid<'a>) (matrix: Mat<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let reduce (monoid: IMonoid<'a>) (matrix: Mat<'a>) : GraphblasEvaluation<Scalar<'a>> =
        failwith "Not Implemented yet"

    let transpose (matrix: Mat<'a>) : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"
//        match matrix with
//        | MatrixCSR matrix ->
//            // map
//            opencl {
//                let! transposed = CSRMatrix.Transpose.transposeMatrix matrix
//                return MatrixCSR transposed
//            }
//        | _ -> failwith "Not Implemented"
//        |> EvalGB.fromCl

    let kronecker
        (semiring: ISemiring<'a>)
        (leftMatrix: Mat<'a>)
        (rightMatrix: Mat<'a>)
        : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"

    (*
        closed masked operations
    *)

    let mxmWithMask
        (semiring: ISemiring<'a>)
        (mask: Mask2D)
        (leftMatrix: Mat<'a>)
        (rightMatrix: Mat<'a>)
        : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"

    let mxvWithMask
        (semiring: ISemiring<'a>)
        (mask: Mask1D)
        (matrix: Mat<'a>)
        (vector: Vector<'a>)
        : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"
//        match matrix, vector, mask with
//        | MatrixCSR matrix, VectorCOO vector, mask when not mask.IsComplemented ->
//            opencl {
//                let! result = CSRMatrix.SpMSpV.masked matrix vector semiring mask
//                return VectorCOO result
//            }
//        | _ -> failwith "Not Implemented"
//        |> EvalGB.fromCl

    let vxmWithMask
        (semiring: ISemiring<'a>)
        (mask: Mask1D)
        (vector: Vector<'a>)
        (matrix: Mat<'a>)
        : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let eWiseAddWithMask
        (monoid: IMonoid<'a>)
        (mask: Mask2D)
        (leftMatrix: Mat<'a>)
        (rightMatrix: Mat<'a>)
        : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"

    let eWiseMultWithMask
        (semiring: ISemiring<'a>)
        (mask: Mask2D)
        (leftMatrix: Mat<'a>)
        (rightMatrix: Mat<'a>)
        : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"

    let applyWithMask (mapper: UnaryOp<'a, 'b>) (mask: Mask2D) (matrix: Mat<'a>) : GraphblasEvaluation<Mat<'b>> =
        failwith "Not Implemented yet"

    let selectWithMask
        (predicate: UnaryOp<int * int * 'a, bool>)
        (mask: Mask2D)
        (matrix: Mat<'a>)
        : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"

    let reduceRowsWithMask (monoid: IMonoid<'a>) (mask: Mask1D) (matrix: Mat<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let reduceColsWithMask (monoid: IMonoid<'a>) (mask: Mask1D) (matrix: Mat<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let kroneckerWithMask
        (semiring: ISemiring<'a>)
        (mask: Mask2D)
        (leftMatrix: Mat<'a>)
        (rightMatrix: Mat<'a>)
        : GraphblasEvaluation<Mat<'a>> =
        failwith "Not Implemented yet"

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module MatrixTuples =
    let synchronize (matrixTuples: Backend.MatrixTuples<'a>) =
        opencl {
            let! _ =
                if matrixTuples.RowIndices.Length = 0 then
                    opencl { return [||] }
                else
                    failwith "FIX ME!"
            //ToHost matrixTuples.RowIndices

            let! _ =
                if matrixTuples.ColumnIndices.Length = 0 then
                    opencl { return [||] }
                else
                    failwith "FIX ME!"
            //ToHost matrixTuples.ColumnIndices

            let! _ =
                if matrixTuples.Values.Length = 0 then
                    opencl { return [||] }
                else
                    failwith "FIX ME!"
            //ToHost matrixTuples.Values

            return ()
        }
        |> EvalGB.fromCl
