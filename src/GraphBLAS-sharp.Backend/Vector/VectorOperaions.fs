namespace GraphBLAS.FSharp.Backend

open GraphBLAS.FSharp.Backend.Common

module VectorOperations =
    let fillSubAddAtLeastOne zero =
        <@
            fun (value: AtLeastOne<'a, 'a>) ->
                let mutable res = zero

                match value with
                | Both (_, right) ->
                    res <- Some right
                | Left left ->
                    res <- Some left
                | Right right ->
                    res <- Some right

                if res = zero then None else Some res
        @>
