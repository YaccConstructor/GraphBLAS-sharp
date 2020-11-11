namespace GraphBLAS.FSharp

type DenseVector<'a when 'a : struct and 'a : equality>(vector: 'a[]) =
    inherit Vector<'a>()

    override this.Length = failwith "Not Implemented"
    override this.AsArray = failwith "Not Implemented"

    override this.Item
        with get (mask: Mask1D<'a>) : Vector<'a> = failwith "Not Implemented"
        and set (mask: Mask1D<'a>) (value: Vector<'a>) = failwith "Not Implemented"
    override this.Item
        with get (rowIdx: int, colIdx: int) : Scalar<'a> = failwith "Not Implemented"
        and set (rowIdx: int, colIdx: int) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Item
        with set (mask: Mask1D<'a>) (value: Scalar<'a>) = failwith "Not Implemented"

    override this.Vxm a b c = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"
    override this.Reduce a = failwith "Not Implemented"

    override this.EWiseAddInplace a b c = failwith "Not Implemented"
    override this.EWiseMultInplace a b c = failwith "Not Implemented"
    override this.ApplyInplace a b = failwith "Not Implemented"
