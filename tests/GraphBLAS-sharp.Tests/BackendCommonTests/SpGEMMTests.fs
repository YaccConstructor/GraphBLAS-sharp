module Backend.SpGEMM

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Tests.Utils

open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp
open OpenCL.Net

open System
open Brahma.FSharp.OpenCL.Shared
open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Utils
open OpenCL.Net
open Backend.Common.StandardOperations

// open GraphBLAS.FSharp.Backend.Common
// open Brahma.FSharp
// open GraphBLAS.FSharp.Backend
// open Microsoft.FSharp.Quotations

let logger = Log.create "SpGEMM.Tests"

let context = defaultContext.ClContext
let config = defaultConfig
let workGroupSize = 32

let tests =

    let config = { defaultConfig with arbitrary = [ typeof<Generators.PairOfMatricesOfCompatibleSize> ] }
    let q = defaultContext.Queue
    q.Error.Add(fun e -> failwithf "%A" e)

    let isEqual = (=)
    let zero = 0

    [
        testPropertyWithConfig config "ff" <| fun (leftMatrix: int [,], rightMatrix: int [,]) ->
            let nrows = Array2D.length1 leftMatrix
            let ncols = Array2D.length2 rightMatrix
            let mask =
                let m = Array2D.create nrows ncols 1
                Mask2D.FromArray2D(m, (fun x -> x = 0))

            let m1 = createMatrixFromArray2D CSR leftMatrix (isEqual zero)
            let m2 = createMatrixFromArray2D CSC rightMatrix (isEqual zero)

            if m1.NNZCount > 0 && m2.NNZCount > 0 then
                let expected =
                    Array2D.init nrows ncols
                    <| fun i j ->
                        (leftMatrix.[i, *], rightMatrix.[*, j])
                        ||> Array.map2 (*)
                        |> Array.reduce (+)
                let expected = createMatrixFromArray2D COO expected (isEqual zero)

                if expected.NNZCount > 0 then
                    // printfn "\n\n\n"
                    // printfn "First array = \n%A" leftMatrix
                    // printfn "Second array = \n%A" rightMatrix
                    // printfn "\n"
                    // printfn "First matrix = \n%A" m1
                    // printfn "Second matrix = \n%A" m2
                    // printfn "Mask = \n%A" mask

                    let m1 = m1.ToBackend context
                    let m2 = m2.ToBackend context
                    let mask = mask.ToBackend context

                    let actual = Matrix.spgemm context workGroupSize intSum intMul q m1 m2 mask

                    let actual = Matrix.FromBackend q actual

                    // Check result
                    "Matrices should be equal"
                    |> Expect.equal actual expected
    ]
    |> testList "SpGEMM tests"
