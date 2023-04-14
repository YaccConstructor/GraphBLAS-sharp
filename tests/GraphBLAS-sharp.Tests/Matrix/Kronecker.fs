module GraphBLAS.FSharp.Tests.Backend.Matrix.kronecker

open Expecto
open Expecto.Logging
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Tests.TestCases
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Objects.MatrixExtensions

let config =
    { Utils.defaultConfig with
          endSize = 100
          maxTest = 50 }

let logger = Log.create "kronecker.Tests"

let workGroupSize = Utils.defaultWorkGroupSize

let makeTest context processor zero isEqual op kroneckerFun (leftMatrix: 'a [,], rightMatrix: 'a [,]) =
    let m1 =
        Utils.createMatrixFromArray2D CSR leftMatrix (isEqual zero)

    let m2 =
        Utils.createMatrixFromArray2D CSR rightMatrix (isEqual zero)

    if m1.NNZ > 0 && m2.NNZ > 0 then
        let expected =
            HostPrimitives.array2DKroneckerProduct leftMatrix rightMatrix op

        let expected =
            Utils.createMatrixFromArray2D COO expected (isEqual zero)

        if expected.NNZ > 0 then

            let m1 = m1.ToDevice context
            let m2 = m2.ToDevice context

            let (result: ClMatrix<'a>) =
                kroneckerFun processor ClContext.HostInterop m1 m2

            let actual = result.ToHost processor

            m1.Dispose processor
            m2.Dispose processor
            result.Dispose processor

            // Check result
            "Matrices should be equal"
            |> Expect.equal actual expected

let createGeneralTest (context: ClContext) (queue: MailboxProcessor<Msg>) (zero: 'a) isEqual op opQ testName =

    let kronecker =
        Matrix.kronecker opQ context workGroupSize

    makeTest context queue zero isEqual op kronecker
    |> testPropertyWithConfig config $"test on %A{typeof<'a>} %s{testName}"

let generalTests (testContext: TestContext) =
    [ let context = testContext.ClContext
      let queue = testContext.Queue
      queue.Error.Add(fun e -> failwithf "%A" e)

      createGeneralTest context queue false (=) (&&) ArithmeticOperations.boolMulOption "bool mul"
      createGeneralTest context queue false (=) (||) ArithmeticOperations.boolSumOption "bool sum"

      createGeneralTest context queue 0 (=) (*) ArithmeticOperations.intMulOption "int mul"
      createGeneralTest context queue 0 (=) (+) ArithmeticOperations.intSumOption "int sum"

      createGeneralTest context queue 0uy (=) (*) ArithmeticOperations.byteMulOption "byte mul"
      createGeneralTest context queue 0uy (=) (+) ArithmeticOperations.byteSumOption "byte sum"

      createGeneralTest context queue 0.0f Utils.float32IsEqual (*) ArithmeticOperations.float32MulOption "float mul"
      createGeneralTest context queue 0.0f Utils.float32IsEqual (+) ArithmeticOperations.float32SumOption "float sum"

      if Utils.isFloat64Available context.ClDevice then
          createGeneralTest context queue 0.0 Utils.floatIsEqual (*) ArithmeticOperations.floatMulOption "float64 mul"
          createGeneralTest context queue 0.0 Utils.floatIsEqual (+) ArithmeticOperations.floatSumOption "float64 sum" ]

let tests =
    gpuTests "Backend.Matrix.kronecker tests" generalTests
