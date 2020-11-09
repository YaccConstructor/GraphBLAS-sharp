namespace GraphBLAS.FSharp

type DenseVector<'a>(vector: 'a[]) =
    inherit Vector<'a>()

    new() = DenseVector(Array.zeroCreate<'a> 0)

    override this.Length = Array.length vector
    override this.AsArray = vector

