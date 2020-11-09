namespace GraphBLAS.FSharp

type DenseVector<'a>(vector: 'a[]) =
    inherit Vector<'a>()

    new() = DenseVector(Array.zeroCreate<'a> 0)

    override this.Length = Array.length vector
    override this.AsArray = vector
    override this.Indices = []

    override this.Item
        with get (mask: Mask1D option) : Vector<'a> = upcast DenseVector<'a>()
        and set (mask: Mask1D option) (value: Vector<'a>) = ()
    override this.Item
        with get (rowIdx: int, colIdx: int) : Scalar<'a> = Scalar Unchecked.defaultof<'a>
        and set (rowIdx: int, colIdx: int) (value: Scalar<'a>) = ()

    override this.Item
        with set (mask: Mask1D option) (value: Scalar<'a>) = ()

    override this.Vxm a b c = upcast DenseVector<'a>()
    override this.EWiseAdd a b c = upcast DenseVector<'a>()
    override this.EWiseMult a b c = upcast DenseVector<'a>()
    override this.Apply<'b> a b = upcast DenseVector<'b>()
    override this.Reduce a = Scalar Unchecked.defaultof<'a>

    override this.EWiseAddInplace a b c = ()
    override this.EWiseMultInplace a b c = ()
    override this.ApplyInplace a b = ()
