namespace GraphBLAS.FSharp

type DenseVector<'a when 'a : struct>(vector: 'a[]) =
    inherit Vector<'a>()

    new() = DenseVector(Array.zeroCreate<'a> 0)
    new(listOfIndices: int list) = DenseVector(Array.zeroCreate<'a> 0)

    override this.Length = failwith "Not Implemented"
    override this.Mask = failwith "Not Implemented"
    override this.AsArray = failwith "Not Implemented"

    override this.Item
        with get (mask: Mask1D option) : Vector<'a> = failwith "Not Implemented"
        and set (mask: Mask1D option) (value: Vector<'a>) = failwith "Not Implemented"
    override this.Item
        with get (rowIdx: int, colIdx: int) : Scalar<'a> = failwith "Not Implemented"
        and set (rowIdx: int, colIdx: int) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Item
        with set (mask: Mask1D option) (value: Scalar<'a>) = failwith "Not Implemented"

    override this.Vxm a b c = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"
    override this.Reduce a = failwith "Not Implemented"

    override this.EWiseAddInplace a b c = failwith "Not Implemented"
    override this.EWiseMultInplace a b c = failwith "Not Implemented"
    override this.ApplyInplace a b = failwith "Not Implemented"
