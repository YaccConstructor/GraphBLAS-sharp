namespace GraphBLAS.FSharp

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp.Backend

type MatrixTuples<'a when 'a : struct and 'a : equality> =
    {
        RowIndices: int[]
        ColumnIndices: int[]
        Values: 'a[]
    }

    member this.ToHost() =
        opencl {
            let! _ = ToHost this.RowIndices
            let! _ = ToHost this.ColumnIndices
            let! _ = ToHost this.Values

            return this
        }

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Matrix =

    (*
        constructors
    *)

    let build (rowCount: int) (columnCount: int) (rows: int[]) (columns: int[]) (values: 'a[]) : Matrix<'a> =
        failwith "Not Implemented yet"

    let ofArray2D (array: 'a[,]) (isZero: 'a -> bool) : Matrix<'a> =
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

    (*
        operations
    *)

    let eWiseAdd (leftMatrix: Matrix<'a>) (rightMatrix: Matrix<'a>) (mask: Mask2D option) (semiring: ISemiring<'a>) =
        let operationResult =
            match leftMatrix, rightMatrix with
            | COOMatrix left, COOMatrix right ->
                opencl {
                    let! result = MatrixCOO.EWiseAdd.run left right mask semiring
                    return COOMatrix(result)
                }
            | _ -> failwith "Not Implemented"

        graphblas { return! EvalGB.liftCl operationResult }
