namespace GraphBLAS.FSharp

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Vector =

    (*
        constructors
    *)

    let build (size: int) (indices: int[]) (values: int[]) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let ofTuples (size: int) (tuples: VectorTuples<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let ofList (size: int) (elements: (int * 'a) list) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let ofArray (array: 'a[]) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let init (size: int) (initializer: int -> 'a) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let create (size: int) (value: 'a) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let zeroCreate (size: int) : GraphblasEvaluation<Vector<'a>> =
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
    let mask (vector: Vector<'a>) : GraphblasEvaluation<Mask1D> = failwith "Not Implemented yet"
    let complemented (vector: Vector<'a>) : GraphblasEvaluation<Mask1D> = failwith "Not Implemented yet"
    let thin (isZero: 'a -> bool) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let switch (vectorType: VectorType) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let synchronize (vector: Vector<'a>) : GraphblasEvaluation<unit> = failwith "Not Implemented yet"

    (*
        assignment, extraction and filling
    *)

    /// vec.[mask]
    let extractSubVector (mask: Mask1D) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    /// vec.[idx]
    let extractValue (idx: int) (vector: Vector<'a>) : GraphblasEvaluation<Scalar<'a>> =
        failwith "Not Implemented yet"

    /// t <- vec
    let assignVector (source: Vector<'a>) (target: Vector<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// t.[mask] <- vec
    let assignSubVector (mask: Mask1D) (source: Vector<'a>) (target: Vector<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// t.[idx] <- value
    let assignValue (idx: int) (value: Scalar<'a>) (target: Vector<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// vec.[*] <- value
    let fillVector (value: Scalar<'a>) (vector: Vector<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// vec.[mask] <- value
    let fillSubVector (mask: Mask1D) (value: Scalar<'a>) (vector: Vector<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    (*
        operations
    *)

    let vxm (semiring: ISemiring<'a>) (vector: Vector<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let eWiseAdd (semiring: ISemiring<'a>) (mask: Mask1D option) (leftVector: Vector<'a>) (rightVector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let eWiseMult (semiring: ISemiring<'a>) (leftVector: Vector<'a>) (rightVector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let apply (mapper: UnaryOp<'a, 'b>) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'b>> = failwith "Not Implemented yet"
    let select (predicate: UnaryOp<'a, bool>) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let reduce (monoid: IMonoid<'a>) (vector: Vector<'a>) : GraphblasEvaluation<Scalar<'a>> = failwith "Not Implemented yet"

    let vxmWithMask (semiring: ISemiring<'a>) (mask: Mask1D) (vector: Vector<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let eWiseAddWithMask (semiring: ISemiring<'a>) (mask: Mask1D) (leftVector: Vector<'a>) (rightVector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let applyWithMask (mapper: UnaryOp<'a, 'b>) (mask: Mask1D) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'b>> = failwith "Not Implemented yet"
    let selectWithMask (predicate: UnaryOp<'a, bool>) (mask: Mask1D) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module VectorTuples =
    let synchronize (vectorTuples: VectorTuples<'a>) =
        opencl {
            let! _ = ToHost vectorTuples.Indices
            let! _ = ToHost vectorTuples.Values
            return ()
        }
        |> EvalGB.fromCl
