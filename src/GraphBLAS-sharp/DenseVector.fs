namespace GraphBLAS.FSharp

open OpenCLContext
open FSharp.Quotations.Evaluator

type DenseVector<'a when 'a : struct and 'a : equality>(vector: 'a[]) =
    inherit Vector<'a>()

    override this.Length = failwith "Not Implemented"
    override this.AsArray = vector

    override this.Assign(mask: Mask1D<'b>, vector: Vector<'a>) : unit = failwith "Not Implemented"
    override this.Assign(idx: int, Scalar (value: 'a)) : unit = failwith "Not Implemented"
    override this.Assign(mask: Mask1D<'b>, Scalar (value: 'a)) : unit =
        match mask with
        | Mask1D maskVector ->
            match maskVector with
            | :? SparseVector<'b> as sparse ->
                sparse.AsList
                |> List.iter (fun (idx, _) -> vector.[idx] <- value)
            | _ -> failwith "Not Implemented"
        | Complemented1D maskVector ->
            match maskVector with
            | :? SparseVector<'b> as sparse -> failwith "Not Implemented"
            | _ -> failwith "Not Implemented"
        | Mask1D.None ->
            [0 .. vector.Length - 1]
            |> List.iter (fun i -> vector.[i] <- value)

    override this.Vxm a b c = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"
    override this.Reduce (monoid: Monoid<'a>) =
        vector
        |> Array.reduce (QuotationEvaluator.Evaluate !> monoid.Append)
        |> Scalar

    override this.EWiseAddInplace a b c = failwith "Not Implemented"
    override this.EWiseMultInplace a b c = failwith "Not Implemented"
    override this.ApplyInplace a b = failwith "Not Implemented"

and SparseVector<'a when 'a : struct and 'a : equality>(size: int, listOfNonzeroes: (int * 'a) list) =
    inherit Vector<'a>()

    override this.Length = failwith "Not Implemented"
    override this.AsArray = failwith "Not Implemented"
    member this.AsList: (int * 'a) list = listOfNonzeroes

    override this.Assign(mask: Mask1D<'b>, vector: Vector<'a>) : unit = failwith "Not Implemented"
    override this.Assign(idx: int, Scalar (value: 'a)) : unit = failwith "Not Implemented"
    override this.Assign(mask: Mask1D<'b>, Scalar (value: 'a)) : unit = failwith "Not Implemented"


    override this.Vxm (matrix: Matrix<'a>) (mask: Mask1D<'b>) (semiring: Semiring<'a>) : Vector<'a> = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"
    override this.Reduce (monoid: Monoid<'a>) = failwith "Not Implemented"

    override this.EWiseAddInplace a b c = failwith "Not Implemented"
    override this.EWiseMultInplace a b c = failwith "Not Implemented"
    override this.ApplyInplace a b = failwith "Not Implemented"
