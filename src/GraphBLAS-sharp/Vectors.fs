namespace GraphBLAS.FSharp

open OpenCLContext
open FSharp.Quotations.Evaluator

type DenseVector<'a when 'a : struct and 'a : equality>(vector: 'a[], monoid: Monoid<'a>) =
    inherit Vector<'a>(vector.Length)

    override this.AsArray = vector

    override this.Extract (mask: Mask1D<'t>) : Vector<'a> = failwith "Not Implemented"
    override this.Extract (idx: int) : Scalar<'a> = failwith "Not Implemented"

    override this.Assign(vector: Vector<'a>, mask: Mask1D<'t>) : unit = failwith "Not Implemented"
    override this.Assign(Scalar (value: 'a), idx: int) : unit = failwith "Not Implemented"
    override this.Assign(Scalar (value: 'a), mask: Mask1D<'t>) : unit =
        match mask with
        | Mask1D maskVector ->
            match maskVector with
            | :? SparseVector<'t> as sparse ->
                sparse.AsList
                |> List.iter (fun (idx, _) -> vector.[idx] <- value)
            | _ -> failwith "Not Implemented"
        | Complemented1D maskVector ->
            match maskVector with
            | :? SparseVector<'t> as sparse -> failwith "Not Implemented"
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

    override this.VxmInplace a b c = failwith "Not Implemented"
    override this.EWiseAddInplace a b c = failwith "Not Implemented"
    override this.EWiseMultInplace a b c = failwith "Not Implemented"
    override this.ApplyInplace a b = failwith "Not Implemented"

and SparseVector<'a when 'a : struct and 'a : equality>(size: int, listOfNonzeroes: (int * 'a) list) =
    inherit Vector<'a>(size)

    member this.AsList: (int * 'a) list = listOfNonzeroes

    override this.AsArray = failwith "Not Implemented"

    override this.Extract (mask: Mask1D<'b>) : Vector<'a> = failwith "Not Implemented"
    override this.Extract (idx: int) : Scalar<'a> = failwith "Not Implemented"

    override this.Assign(vector: Vector<'a>, mask: Mask1D<'b>) : unit = failwith "Not Implemented"
    override this.Assign(Scalar (value: 'a), idx: int) : unit = failwith "Not Implemented"
    override this.Assign(Scalar (value: 'a), mask: Mask1D<'b>) : unit = failwith "Not Implemented"

    override this.Vxm (matrix: Matrix<'b>) (mask: Mask1D<'t>) (semiring: Semiring<'a, 'b, 'c>) : Vector<'c> = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"
    override this.Reduce (monoid: Monoid<'a>) = failwith "Not Implemented"

    override this.VxmInplace a b c = failwith "Not Implemented"
    override this.EWiseAddInplace a b c = failwith "Not Implemented"
    override this.EWiseMultInplace a b c = failwith "Not Implemented"
    override this.ApplyInplace a b = failwith "Not Implemented"
