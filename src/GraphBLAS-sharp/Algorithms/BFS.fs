namespace GraphBLAS.FSharp

open GraphBLAS.FSharp.Predefined
open OpenCLContext

module Algorithms =

    let bfs (matrix: Matrix<bool>, source: int) =
        let n = matrix.RowCount
        let v = DenseVector(Array.zeroCreate n)
        let q = SparseVector(n, [])
        q.Assign(source, Scalar true)

        let mutable d = 1
        let mutable succ = true

        while succ && d <= n do
            v.Assign(Mask1D q, Scalar d)
            q.Assign(Complemented1D v, q.Vxm matrix Mask1D.None BooleanSemiring.OrAnd)
            succ <- !> (q.Reduce BooleanMonoid.Or)
            d <- d + 1
        v
