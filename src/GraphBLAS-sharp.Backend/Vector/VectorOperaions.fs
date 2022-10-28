namespace GraphBLAS.FSharp.Backend

open GraphBLAS.FSharp.Backend.Common

module VectorOperations =
    let fillSubAddAtLeastOne zero =
        <@
            fun (value: AtLeastOne<'a, 'a>) ->
                let mutable res = zero

                match value with
                | Both (_, right) ->
                    res <- right
                | Left left ->
                    res <- left
                | Right right ->
                    res <- right

                if res = zero then None else Some res
        @>
