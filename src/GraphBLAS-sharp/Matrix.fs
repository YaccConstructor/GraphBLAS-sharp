namespace GraphBLAS.FSharp

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp.Backend

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

    let eWiseAdd (matrix: Matrix<'a>) (cooFormat: COOFormat<'a>) (mask: Mask2D option) (sr: ISemiring<'a>) =
        let s =
            match matrix with
            | :? COOMatrix<'a> as coo ->
                opencl {
                    let! cooFormat = MatrixCOO.EWiseAdd.run cooFormat coo.Storage mask sr
                    return COOMatrix(cooFormat) :> Matrix<'a>
                }
            | _ -> failwith "Not Implemented"

        graphblas {
            let! m = EvalGB.liftCl s
            return m
        }
