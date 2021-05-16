namespace GraphBLAS.FSharp

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.COOVector

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Vector =

    (*
        constructors
    *)

    let build (size: int) (indices: int[]) (values: 'a[]) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let ofTuples (size: int) (tuples: VectorTuples<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let ofList (size: int) (elements: (int * 'a) list) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    // можно оставить, но с условием, что будет создаваться full vector
    // let ofArray (array: 'a[]) : GraphblasEvaluation<Vector<'a>> =
    //     failwith "Not Implemented yet"

    let init (size: int) (initializer: int -> 'a) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let create (size: int) (value: 'a) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let zeroCreate<'a when 'a : struct> (size: int) : GraphblasEvaluation<Vector<'a>> =
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

    let mask (vector: Vector<'a>) : GraphblasEvaluation<Mask1D> =
        match vector with
        | VectorCOO v ->
            graphblas {
                let! resultIndices = Copy.run v.Indices |> EvalGB.fromCl
                return Mask1D(resultIndices, v.Size, false)
            }

    let complemented (vector: Vector<'a>) : GraphblasEvaluation<Mask1D> =
        match vector with
        | VectorCOO v ->
            graphblas {
                let! resultIndices = Copy.run v.Indices |> EvalGB.fromCl
                return Mask1D(resultIndices, v.Size, true)
            }

    let thin (isZero: 'a -> bool) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let switch (vectorType: VectorType) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"

    let synchronize (vector: Vector<'a>) : GraphblasEvaluation<unit> =
        match vector with
        | VectorCOO v ->
            opencl {
                let! _ = ToHost v.Indices
                let! _ = ToHost v.Values
                return ()
            }
            |> EvalGB.fromCl

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
        match source, target with
        | VectorCOO s, VectorCOO t ->
            if t.Size <> mask.Size then
                invalidArg "mask" <| sprintf "The size of mask must be %A. Received: %A" t.Size mask.Size

            if t.Size <> s.Size then
                invalidArg "source" <| sprintf "The size of source vector must be %A. Received: %A" t.Size s.Size

            if mask.IsComplemented then failwith "Not Implemented yet"
            else
                graphblas {
                    let! resultIndices, resultValues = AssignSubVector.run t.Indices t.Values s.Indices s.Values mask.Indices |> EvalGB.fromCl
                    t.Indices <- resultIndices
                    t.Values <- resultValues
                }

    /// t.[idx] <- value
    let assignValue (idx: int) (value: Scalar<'a>) (target: Vector<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// vec.[*] <- value
    let fillVector (value: Scalar<'a>) (vector: Vector<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// vec.[mask] <- value
    let fillSubVector (mask: Mask1D) ((Scalar scalar): Scalar<'a>) (vector: Vector<'a>) : GraphblasEvaluation<unit> =
        match vector with
        | VectorCOO v ->
            if mask.IsComplemented then failwith "Not Implemented yet" else
                graphblas {
                    let! resultIndices, resultValues = FillSubVector.run v.Indices v.Values mask.Indices scalar.Value |> EvalGB.fromCl
                    v.Indices <- resultIndices
                    v.Values <- resultValues
                }

    (*
        operations
    *)

    let vxm (semiring: ISemiring<'a>) (vector: Vector<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let eWiseAdd (monoid: IMonoid<'a>) (mask: Mask1D option) (leftVector: Vector<'a>) (rightVector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let eWiseMult (semiring: ISemiring<'a>) (leftVector: Vector<'a>) (rightVector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let apply (mapper: UnaryOp<'a, 'b>) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'b>> = failwith "Not Implemented yet"
    let select (predicate: UnaryOp<'a, bool>) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"

    let reduce (monoid: IMonoid<'a>) (vector: Vector<'a>) : GraphblasEvaluation<Scalar<'a>> =
        match vector with
        | VectorCOO v ->
            graphblas {
                let! result =
                    opencl {
                        let (ClosedBinaryOp plus) = monoid.Plus
                        return! Sum.run v.Values plus monoid.Zero
                    }
                    |> EvalGB.fromCl

                return Scalar {Value = result}
            }

    let vxmWithMask (semiring: ISemiring<'a>) (mask: Mask1D) (vector: Vector<'a>) (matrix: Matrix<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let eWiseAddWithMask (monoid: IMonoid<'a>) (mask: Mask1D) (leftVector: Vector<'a>) (rightVector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let eWiseMultWithMask (semiring: ISemiring<'a>) (mask: Mask1D) (leftVector: Vector<'a>) (rightVector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
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
