namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp

module BetweennessCentrality =
    // NOTE matrix of bool?
    let metric (matrix: Matrix<int>) (source: int) = graphblas {
        let n = Matrix.rowCount matrix
        let! delta = Vector.zeroCreate<float32> n
        let! sigma = Matrix.zeroCreate<int> n n
        let! q = Vector.ofList n [source, 1]
        let! p = Vector.copy q

        let! pMask = Vector.complemented p
        do! Vector.vxmWithMask AddMult.int pMask q matrix
        >>= Vector.assignVector q

        let mutable d = 0
        let mutable sum = 0
        let mutable break' = false

        while not break' || sum <> 0 do
            break' <- true

            do! Matrix.assignRow sigma d q

            do! Vector.eWiseAdd Add.int p q
            >>= Vector.assignVector p // ?

            let! pMask = Vector.complemented p
            do! Vector.vxmWithMask AddMult.int pMask q matrix
            >>= Vector.assignVector q

            do! Vector.reduce Add.int q
            >>= fun (Scalar s) -> EvalGB.return' (sum <- s)

            d <- d + 1

        let! t1 = Vector.zeroCreate<float32> n
        let! t2 = Vector.zeroCreate<float32> n
        let! t3 = Vector.zeroCreate<float32> n
        let! t4 = Vector.zeroCreate<float32> n

        for i = d - 1 downto 1 do
            // t1 <- 1 + delta
            do! Vector.apply (UnaryOp <@ (+) 1.f @>) delta
            >>= Vector.assignVector t1

            // t2 <- sigma.[i, *]
            do! Matrix.extractRow sigma i
            >>= Vector.apply (UnaryOp <@ float32 @>)
            >>= Vector.assignVector t2

            // t2 <- t1 / t2
            let! qMask = Vector.mask q
            do! Vector.apply (UnaryOp <@ (/) 1.f @>) t2
            >>= fun x -> Vector.eWiseMultWithMask AddMult.float32 qMask t1 x
            >>= Vector.assignVector t2

            do! Matrix.apply (UnaryOp <@ float32 @>) matrix
            >>= fun matrix -> Matrix.mxv AddMult.float32 matrix t2
            >>= Vector.assignVector t3

            // t4 <- sigma.[i - 1, *] * t3
            do! Matrix.extractRow sigma (i - 1)
            >>= Vector.apply (UnaryOp <@ float32 @>)
            >>= fun x -> Vector.eWiseMult AddMult.float32 x t3
            >>= Vector.assignVector t4

            // delta <- delta + t4
            do! Vector.eWiseAdd Add.float32 delta t4
            >>= Vector.assignVector delta

        return delta
    }
