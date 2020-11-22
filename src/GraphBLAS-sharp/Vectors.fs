namespace GraphBLAS.FSharp

open OpenCLContext
open FSharp.Quotations.Evaluator

type DenseVector<'a when 'a : struct and 'a : equality>(vector: 'a[], monoid: Monoid<'a>) =
    inherit Vector<'a>(vector.Length)

    override this.AsArray = vector
    override this.Clear () = failwith "Not Implemented"

    override this.Item
        with get (mask: Mask1D) : Vector<'a> = failwith "Not Implemented"
        and set (mask: Mask1D) (value: Vector<'a>) = failwith "Not Implemented"
    override this.Item
        with get (idx: int) : Scalar<'a> = failwith "Not Implemented"
        and set (idx: int) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Fill
        with set (mask: Mask1D) (value: Scalar<'a>) = failwith "Not Implemented"

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
    override this.Clear () = failwith "Not Implemented"

    override this.Item
        with get (mask: Mask1D) : Vector<'a> = failwith "Not Implemented"
        and set (mask: Mask1D) (value: Vector<'a>) = failwith "Not Implemented"
    override this.Item
        with get (idx: int) : Scalar<'a> = failwith "Not Implemented"
        and set (idx: int) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Fill
        with set (mask: Mask1D) (value: Scalar<'a>) = failwith "Not Implemented"

    override this.Vxm (matrix: Matrix<'b>) (mask: Mask1D) (semiring: Semiring<'a, 'b, 'c>) : Vector<'c> = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"
    override this.Reduce (monoid: Monoid<'a>) = failwith "Not Implemented"

    override this.VxmInplace a b c = failwith "Not Implemented"
    override this.EWiseAddInplace a b c = failwith "Not Implemented"
    override this.EWiseMultInplace a b c = failwith "Not Implemented"
    override this.ApplyInplace a b = failwith "Not Implemented"
