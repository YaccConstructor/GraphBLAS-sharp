namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open Microsoft.FSharp.Quotations

module CSRMatrix =
    let toCOO = Converter.toCOO

    let eWiseAdd (clContext: ClContext) (opAdd: Expr<'a -> 'a -> 'a>) workGroupSize =

        let toCOO = toCOO clContext

        let eWiseCOO =
            COOMatrix.eWiseAdd clContext opAdd workGroupSize

        let toCSR = COOMatrix.toCSR clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (m1: CSRMatrix<'a>) (m2: CSRMatrix<'a>) ->

            let m1COO = toCOO processor workGroupSize m1
            let m2COO = toCOO processor workGroupSize m2

            let m3COO = eWiseCOO processor m1COO m2COO

            let m3 = toCSR processor m3COO

            m3
