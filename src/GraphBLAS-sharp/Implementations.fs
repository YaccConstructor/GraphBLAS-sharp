namespace GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend

type CSRMatrix<'a when 'a : struct and 'a : equality>(csrTuples: CSRFormat<'a>) =
    inherit Matrix<'a>(csrTuples.RowPointers.Length - 1, csrTuples.ColumnCount)

    let rowCount = base.RowCount
    let columnCount = base.ColumnCount

    new(rows: int[], columns: int[], values: 'a[]) = CSRMatrix(CSRFormat.CreateEmpty())
    new(pathToMatrix: string) = CSRMatrix(CSRFormat.CreateEmpty())

    member this.Values = csrTuples.Values
    member this.Columns = csrTuples.ColumnIndices
    member this.RowPointers = csrTuples.RowPointers

    override this.Clear () = failwith "Not Implemented"
    override this.Copy () = failwith "Not Implemented"
    override this.Resize a b = failwith "Not Implemented"
    override this.GetNNZ () = failwith "Not Implemented"
    override this.GetTuples () = failwith "Not Implemented"
    override this.GetMask(?isComplemented: bool) =
        let isComplemented = defaultArg isComplemented false
        failwith "Not Implemented"
    override this.ToHost () = failwith "Not implemented"

    override this.Extract (mask: Mask2D option) : OpenCLEvaluation<Matrix<'a>> = failwith "Not Implemented"
    override this.Extract (colMask: Mask1D option * int) : OpenCLEvaluation<Vector<'a>> = failwith "Not Implemented"
    override this.Extract (rowMask: int * Mask1D option) : OpenCLEvaluation<Vector<'a>> = failwith "Not Implemented"
    override this.Extract (idx: int * int) : OpenCLEvaluation<Scalar<'a>> = failwith "Not Implemented"
    override this.Assign (mask: Mask2D option, value: Matrix<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (colMask: Mask1D option * int, value: Vector<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (rowMask: int * Mask1D option, value: Vector<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (idx: int * int, value: Scalar<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (mask: Mask2D option, value: Scalar<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (colMask: Mask1D option * int, value: Scalar<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (rowMask: int * Mask1D option, value: Scalar<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"

    override this.Mxm a b c = failwith "Not Implemented"
    override this.Mxv a b c = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b  = failwith "Not Implemented"
    override this.Prune a b = failwith "Not Implemented"
    override this.ReduceIn a b = failwith "Not Implemented"
    override this.ReduceOut a b = failwith "Not Implemented"
    override this.Reduce a = failwith "Not Implemented"
    override this.Transpose () = failwith "Not Implemented"
    override this.Kronecker a b c = failwith "Not Implemented"

and COOMatrix<'a when 'a : struct and 'a : equality>(cooFormat: COOFormat<'a>) =
    inherit Matrix<'a>(cooFormat.RowCount, cooFormat.ColumnCount)

    new(rowCount: int, columnCount: int, rows: int[], columns: int[], values: 'a[]) =
        let cooFormat = {
            RowCount = rowCount
            ColumnCount = columnCount
            Rows = rows
            Columns = columns
            Values = values
        }

        COOMatrix(cooFormat)

    new(array: 'a[,], isZero: 'a -> bool) =
        let (rows, cols, vals) =
            array
            |> Seq.cast<'a>
            |> Seq.mapi (fun idx v -> (idx / Array2D.length2 array, idx % Array2D.length2 array, v))
            |> Seq.filter (fun (i, j, v) -> not <| isZero v)
            |> Array.ofSeq
            |> Array.unzip3

        COOMatrix(Array2D.length1 array, Array2D.length2 array, rows, cols, vals)

    override this.ToString() =
        [
            sprintf "COO Matrix %ix%i \n" cooFormat.RowCount cooFormat.ColumnCount
            sprintf "RowIndices:    %A \n" cooFormat.Rows
            sprintf "ColumnIndices: %A \n" cooFormat.Columns
            sprintf "Values:        %A \n" cooFormat.Values
        ]
        |> String.concat ""

    member this.Storage = cooFormat

    member this.Rows with get() = cooFormat.Rows
    member this.Columns with get() = cooFormat.Columns
    member this.Values with get() = cooFormat.Values
    member this.Elements with get() = (cooFormat.Rows, cooFormat.Columns, cooFormat.Values) |||> Array.zip3

    override this.Clear () = failwith "Not Implemented"
    override this.Copy () = failwith "Not Implemented"
    override this.Resize a b = failwith "Not Implemented"
    override this.GetNNZ () = failwith "Not Implemented"

    override this.GetTuples() = opencl {
        return {
            RowIndices = this.Rows
            ColumnIndices = this.Columns
            Values = this.Values
        }
    }

    override this.GetMask(?isComplemented: bool) =
        let isComplemented = defaultArg isComplemented false
        failwith "Not Implemented"

    override this.ToHost() = opencl {
        let! _ = ToHost this.Rows
        let! _ = ToHost this.Columns
        let! _ = ToHost this.Values

        return upcast this
    }

    override this.Extract (mask: Mask2D option) : OpenCLEvaluation<Matrix<'a>> = failwith "Not Implemented"
    override this.Extract (colMask: Mask1D option * int) : OpenCLEvaluation<Vector<'a>> = failwith "Not Implemented"
    override this.Extract (rowMask: int * Mask1D option) : OpenCLEvaluation<Vector<'a>> = failwith "Not Implemented"
    override this.Extract (idx: int * int) : OpenCLEvaluation<Scalar<'a>> = failwith "Not Implemented"
    override this.Assign (mask: Mask2D option, value: Matrix<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (colMask: Mask1D option * int, value: Vector<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (rowMask: int * Mask1D option, value: Vector<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (idx: int * int, value: Scalar<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (mask: Mask2D option, value: Scalar<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (colMask: Mask1D option * int, value: Scalar<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (rowMask: int * Mask1D option, value: Scalar<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"

    override this.Mxm a b c = failwith "Not Implemented"
    override this.Mxv a b c = failwith "Not Implemented"

    override this.EWiseAdd
        (matrix: Matrix<'a>)
        (mask: Mask2D option)
        (semiring: Semiring<'a>) =

        if (this.RowCount, this.ColumnCount) <> (matrix.RowCount, matrix.ColumnCount) then
            invalidArg
                "matrix"
                (sprintf "Argument has invalid dimension. Need %A, but given %A" (this.RowCount, this.ColumnCount) (matrix.RowCount, matrix.ColumnCount))

        // let mask =
        //     match matrixMask with
        //     | Some m ->
        //         if (m.RowCount, m.ColumnCount) <> (this.RowCount, this.ColumnCount) then
        //             invalidArg
        //                 "mask"
        //                 (sprintf "Argument has invalid dimension. Need %A, but given %A" (this.RowCount, this.ColumnCount) (m.RowCount, m.ColumnCount))
        //         m
        //     | _ -> Mask2D(Array.empty, this.RowCount, this.ColumnCount, true) // Empty complemented mask is equal to none

        match matrix with
        | :? COOMatrix<'a> as coo ->
            opencl {
                let! cooFormat = EWiseAdd.coo this.Storage coo.Storage mask semiring
                return upcast COOMatrix(cooFormat)
            }
        | _ -> failwith "Not Implemented"

    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b  = failwith "Not Implemented"
    override this.Prune a b = failwith "Not Implemented"
    override this.ReduceIn a b = failwith "Not Implemented"
    override this.ReduceOut a b = failwith "Not Implemented"
    override this.Reduce a = failwith "Not Implemented"
    override this.Transpose () = failwith "Not Implemented"
    override this.Kronecker a b c = failwith "Not Implemented"

and SparseVector<'a when 'a : struct and 'a : equality>(size: int, indices: int[], values: 'a[]) =
    inherit Vector<'a>(size)

    let mutable indices = indices
    let mutable values = values

    override this.ToString() =
        [
            sprintf "Sparse Vector\n"
            sprintf "Size: %i\n" this.Size
            sprintf "Indices: %A \n" this.Indices
            sprintf "Values:  %A \n" this.Values
        ]
        |> String.concat ""

    member this.Values with get() = values
    member this.Indices with get() = indices
    member this.Elements with get() = (indices, values) ||> Array.zip

    override this.Clear () = failwith "Not Implemented"
    override this.Copy () = failwith "Not Implemented"
    override this.Resize a = failwith "Not Implemented"
    override this.GetNNZ () = failwith "Not Implemented"

    override this.GetTuples () =
        opencl {
            return {| Indices = this.Indices; Values = this.Values |}
        }

    override this.GetMask(?isComplemented: bool) =
        let isComplemented = defaultArg isComplemented false
        failwith "Not Implemented"

    override this.ToHost () =
        opencl {
            let! _ = ToHost this.Indices
            let! _ = ToHost this.Values

            return upcast this
        }

    override this.Extract (mask: Mask1D option) : OpenCLEvaluation<Vector<'a>> = failwith "Not Implemented"
    override this.Extract (idx: int) : OpenCLEvaluation<Scalar<'a>> = failwith "Not Implemented"
    override this.Assign (mask: Mask1D option, vector: Vector<'a>) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (idx: int, Scalar (value: 'a)) : OpenCLEvaluation<unit> = failwith "Not Implemented"
    override this.Assign (mask: Mask1D option, Scalar (value: 'a)) : OpenCLEvaluation<unit> = failwith "Not Implemented"

    override this.Vxm (matrix: Matrix<'a>) (mask: Mask1D option) (semiring: Semiring<'a>) : OpenCLEvaluation<Vector<'a>> = failwith "Not Implemented"

    override this.EWiseAdd
        (vector: Vector<'a>)
        (mask: Mask1D option)
        (semiring: Semiring<'a>) =

        if vector.Size <> this.Size then
            invalidArg
                "vector"
                (sprintf "Argument has invalid dimension. Need %i, but given %i" this.Size vector.Size)

        // let mask =
        //     match mask with
        //     | Some m ->
        //         if m.Size <> this.Size then
        //             invalidArg
        //                 "mask"
        //                 (sprintf "Argument has invalid dimension. Need %i, but given %i" this.Size m.Size)
        //         m
        //     | _ -> Mask1D(Array.empty, this.Size, true) // Empty complemented mask is equal to none

        match vector with
        | :? SparseVector<'a> as sparse ->
            opencl {
                let! resultIndices, resultValues = EWiseAdd.sparse this.Indices this.Values sparse.Indices sparse.Values mask semiring
                return upcast SparseVector(this.Size, resultIndices, resultValues)
            }
        | _ -> failwith "Not Implemented"

    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"
    override this.Prune a b = failwith "Not Implemented"
    override this.Reduce (monoid: Monoid<'a>) = failwith "Not Implemented"
