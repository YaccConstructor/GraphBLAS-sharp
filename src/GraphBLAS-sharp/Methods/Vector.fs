namespace GraphBLAS.FSharp

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp.Backend

type VectorTuples<'a> =
    {
        Indices: int[]
        Values: 'a[]
    }

module VectorTuples =
    let synchronize (vectorTuples: VectorTuples<'a>) =
        opencl {
            let! _ = ToHost vectorTuples.Indices
            let! _ = ToHost vectorTuples.Values
            return ()
        }
        |> EvalGB.fromCl

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Vector =

    (*
        constructors
    *)

    let build (size: int) (indices: int[]) (values: int[]) : Vector<'a> =
        failwith "Not Implemented yet"

    let ofList (size: int) (elements: (int * 'a) list) : Vector<'a> =
        failwith "Not Implemented yet"

    let ofArray (isZero: 'a -> bool) (array: 'a[]) : Vector<'a> =
        failwith "Not Implemented yet"

    let init (size: int) (initializer: int -> 'a) : Vector<'a> =
        failwith "Not Implemented yet"

    // обоснован ли этот метод или можно просто create x использовать
    let zeroCreate (size: int) : Vector<'a> =
        failwith "Not Implemented yet"

    (*
        methods
    *)

    let size (vector: Vector<'a>) : int = failwith "Not Implemented yet"
    let clear (vector: Vector<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"
    let copy (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let resize (size: int) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let nnz (vector: Vector<'a>) : GraphblasEvaluation<int> = failwith "Not Implemented yet"
    let tuples (vector: Vector<'a>) : GraphblasEvaluation<VectorTuples<'a>> = failwith "Not Implemented yet"
    // возвращается option, чтобы потом можно было бы сразу передавать её в методы (тк они option принимают)
    let mask (vector: Vector<'a>) : GraphblasEvaluation<Mask1D option> = failwith "Not Implemented yet"
    let complemented (vector: Vector<'a>) : GraphblasEvaluation<Mask1D option> = failwith "Not Implemented yet"
    let synchronize (vector: Vector<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"

    (*
        assignment, extraction and filling
    *)

    // добавить функции, чтобы разделить поведение при наличии и отсутствии маски (assignSubVector и assignVector)
    let extractSubVector (mask: Mask1D option) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let extractElement (idx: int) (vector: Vector<'a>) : GraphblasEvaluation<Scalar<'a>> = failwith "Not Implemented yet"
    let assignSubVector (mask: Mask1D option) (source: Vector<'a>) (target: Vector<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"
    let assignElement (idx: int) (source: Scalar<'a>) (target: Vector<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"
    let fillSubVector (mask: Mask1D option) (filler: Scalar<'a>) (vector: Vector<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"

    (*
        operations
    *)

    let vxm (semiring: ISemiring<'a>) (mask: Mask1D option) (vector: Vector<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let eWiseAdd (semiring: ISemiring<'a>) (mask: Mask1D option) (leftVector: Vector<'a>) (rightVector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let eWiseMult (semiring: ISemiring<'a>) (mask: Mask2D option) (leftVector: Vector<'a>) (rightVector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let apply (mapper: UnaryOp<'a, 'b>) (mask: Mask1D option) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'b>> = failwith "Not Implemented yet"
    let prune (predicate: UnaryOp<'a, bool>) (mask: Mask1D option) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let reduce (monoid: IMonoid<'a>) (vector: Vector<'a>) : GraphblasEvaluation<Scalar<'a>> = failwith "Not Implemented yet"
