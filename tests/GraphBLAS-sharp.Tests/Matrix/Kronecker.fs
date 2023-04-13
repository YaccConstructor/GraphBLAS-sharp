module GraphBLAS.FSharp.Tests.Backend.Matrix.kronecker

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Objects.MatrixExtensions

let logger = Log.create "kronecker.Tests"

let context = defaultContext.ClContext
let workGroupSize = Utils.defaultWorkGroupSize

let makeTest context q zero isEqual mul kroneckerFun (leftMatrix: 'a [,], rightMatrix: 'a [,]) =

    let m1 =
        Utils.createMatrixFromArray2D CSR leftMatrix (isEqual zero)

    let m2 =
        Utils.createMatrixFromArray2D CSR rightMatrix (isEqual zero)

    if m1.NNZ > 0 && m2.NNZ > 0 then
        let expected =
            Array2D.init
            <| (Array2D.length1 leftMatrix)
               * (Array2D.length1 rightMatrix)
            <| (Array2D.length2 leftMatrix)
               * (Array2D.length2 rightMatrix)
            <| fun i j ->
                let leftElement =
                    leftMatrix.[i / (Array2D.length1 rightMatrix), j / (Array2D.length2 rightMatrix)]

                let rightElement =
                    rightMatrix.[i % (Array2D.length1 rightMatrix), j % (Array2D.length2 rightMatrix)]

                mul leftElement rightElement

        let expected =
            Utils.createMatrixFromArray2D COO expected (isEqual zero)

        if expected.NNZ > 0 then
            let m1 = m1.ToDevice context
            let m2 = m2.ToDevice context

            let (result: ClMatrix<'a>) =
                kroneckerFun q ClContext.HostInterop m1 m2

            let actual = result.ToHost q

            m1.Dispose q
            m2.Dispose q
            result.Dispose q

            // Check result
            "Matrices should be equal"
            |> Expect.equal actual expected

let tests =
    let getCorrectnessTestName = sprintf "Correctness on %s"

    let config = Utils.defaultConfig

    let q = defaultContext.Queue
    q.Error.Add(fun e -> failwithf "%A" e)

    [ let kroneckerMul =
          Matrix.kronecker ArithmeticOperations.intMul context workGroupSize

      makeTest context q 0 (=) (*) kroneckerMul
      |> testPropertyWithConfig config (getCorrectnessTestName "int mul")

      let kroneckerSum =
          Matrix.kronecker ArithmeticOperations.intSum context workGroupSize

      makeTest context q 0 (=) (+) kroneckerSum
      |> testPropertyWithConfig config (getCorrectnessTestName "int sum")

      let kroneckerFun =
          Matrix.kronecker ArithmeticOperations.boolMul context workGroupSize

      makeTest context q false (=) (&&) kroneckerFun
      |> testPropertyWithConfig config (getCorrectnessTestName "bool mul")

      let kroneckerFun =
          Matrix.kronecker ArithmeticOperations.boolSum context workGroupSize

      makeTest context q false (=) (||) kroneckerFun
      |> testPropertyWithConfig config (getCorrectnessTestName "bool sum")
       ]
    |> testList "kronecker masked tests"
