namespace GraphBLAS.FSharp

type DenseVector<'a when 'a : struct and 'a : equality>(vector: 'a[], monoid: Monoid<'a>) =
    inherit Vector<'a>()

    new(monoid: Monoid<'a>) = DenseVector(Array.zeroCreate<'a> 0, monoid)
    new(listOfIndices: int list, monoid: Monoid<'a>) = DenseVector(Array.zeroCreate<'a> 0, monoid)

    member this.Monoid = monoid
    member this.Values = vector

    override this.Length = failwith "Not Implemented"
    override this.AsArray = failwith "Not Implemented"

    override this.CreateMask (isRegular: bool) =
        let indices =
            [| for i in 0 .. this.Length - 1 do
                if this.Values.[i] <> this.Monoid.Zero then yield i |]
        Mask1D(false, indices, this.Length, isRegular)

    override this.Item
        with get (mask: Mask1D) : Vector<'a> = failwith "Not Implemented"
        and set (mask: Mask1D) (value: Vector<'a>) = failwith "Not Implemented"
    override this.Item
        with get (idx: int) : Scalar<'a> = failwith "Not Implemented"
        and set (idx: int) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Item
        with set (mask: Mask1D) (value: Scalar<'a>) = failwith "Not Implemented"

    override this.Vxm a b c = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"
    override this.Reduce a = failwith "Not Implemented"

    override this.EWiseAddInplace a b c = failwith "Not Implemented"
    override this.EWiseMultInplace a b c = failwith "Not Implemented"
    override this.ApplyInplace a b = failwith "Not Implemented"
