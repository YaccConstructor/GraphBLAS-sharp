namespace GraphBLAS.FSharp

type DenseVector<'a>(vector: 'a[]) =
    inherit Vector<'a>()

    override this.Length = Array.length vector
