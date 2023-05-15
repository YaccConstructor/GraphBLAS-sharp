﻿module GraphBLAS.FSharp.Tests.Backend.Vector.SpMSpV

open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open Expecto
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Tests.TestCases
open Microsoft.FSharp.Collections
open Microsoft.FSharp.Core
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects.ClContext

let config = Utils.defaultConfig

let wgSize = Utils.defaultWorkGroupSize

let checkResult
    sumOp
    mulOp
    (zero: 'a)
    (baseMtx: 'a [,])
    (baseVtr: 'a [])
    (actualIndices: int [])
    (actualValues: 'a [])
    =
    let rows = Array2D.length1 baseMtx
    let columns = Array2D.length2 baseMtx

    let expectedV = Array.create columns zero
    let mutable expectedIndices = List.Empty
    let mutable expectedValues = List.Empty

    for c in 0 .. columns - 1 do
        let mutable sum = zero

        for r in 0 .. rows - 1 do
            sum <- sumOp sum (mulOp baseMtx.[r, c] baseVtr.[r])

        expectedV.[c] <- sum

    for i in 0 .. columns - 1 do
        if expectedV.[i] <> zero then
            expectedIndices <- List.append expectedIndices [ i ]
            expectedValues <- List.append expectedValues [ expectedV.[i] ]

    Expect.sequenceEqual
        actualIndices
        expectedIndices
        $"Values should be the same. Actual is {actualIndices}, expected {expectedIndices}."

    Expect.sequenceEqual
        actualValues
        expectedValues
        $"Values should be the same. Actual is {actualValues}, expected {expectedValues}."

let correctnessGenericTest
    (zero: 'a)
    some
    sumOp
    mulOp
    (spMV: MailboxProcessor<_> -> ClMatrix.CSR<'a> -> ClVector.Sparse<'a> -> ClVector.Sparse<'a>)
    (isEqual: 'a -> 'a -> bool)
    q
    (testContext: TestContext)
    (vector: 'a [], matrix: 'a [,], _: bool [])
    =

    if (Array2D.length1 matrix > 0 && vector.Length > 0) then
        //Ensure that result is not empty
        vector.[0] <- some
        matrix.[0, 0] <- some

        let mtx =
            Utils.createMatrixFromArray2D CSR matrix (isEqual zero)

        let vtr =
            Utils.createVectorFromArray Sparse vector (isEqual zero)

        if mtx.NNZ > 0 && vtr.Size > 0 then
            try
                let m = mtx.ToDevice testContext.ClContext

                match vtr, m with
                | Vector.Sparse vtr, ClMatrix.CSR m ->
                    let v = vtr.ToDevice testContext.ClContext

                    let res = spMV testContext.Queue m v

                    (ClMatrix.CSR m).Dispose q
                    v.Dispose q
                    let hostResIndices = res.Indices.ToHost q
                    let hostResValues = res.Values.ToHost q
                    res.Dispose q

                    checkResult sumOp mulOp zero matrix vector hostResIndices hostResValues
                | _ -> failwith "Impossible"
            with
            | ex when ex.Message = "InvalidBufferSize" -> ()
            | ex -> raise ex

let createTest testContext (zero: 'a) some isEqual add mul addQ mulQ =
    let context = testContext.ClContext
    let q = testContext.Queue

    let getCorrectnessTestName datatype =
        $"Correctness on %s{datatype}, %A{testContext.ClContext}"

    let spMSpV = SpMSpV.run context addQ mulQ wgSize

    testContext
    |> correctnessGenericTest zero some add mul spMSpV isEqual q
    |> testPropertyWithConfig config (getCorrectnessTestName $"{typeof<'a>}")


let testFixturesSpMSpV (testContext: TestContext) =
    [ let context = testContext.ClContext
      let q = testContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      createTest testContext false true (=) (||) (&&) ArithmeticOperations.boolSum ArithmeticOperations.boolMul
      createTest testContext 0 1 (=) (+) (*) ArithmeticOperations.intSum ArithmeticOperations.intMul
      createTest testContext 0.0f 1f (=) (+) (*) ArithmeticOperations.float32Sum ArithmeticOperations.float32Mul ]

let tests =
    gpuTests "Backend.Vector.SpMSpV tests" testFixturesSpMSpV
