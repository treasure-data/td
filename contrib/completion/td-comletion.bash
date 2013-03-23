# bash completion for td commands

_td()
{
    COMP_WORDBREAKS=${COMP_WORDBREAKS//:}
    local cur="${COMP_WORDS[COMP_CWORD]}"
    local prev="${COMP_WORDS[COMP_CWORD - 1]}"
    local list="`td help:all | awk '{print $1}' | grep '^[a-z]' | xargs`"

    # echo "cur=$cur, prev=$prev"

    if [[ "$prev" == "td" ]]; then
        if [[ "$cur" == "" ]]; then
            COMPREPLY=($list)
        else
            COMPREPLY=($(compgen -W "$list" -- "$cur"))
        fi
    fi
}
complete -F _td td

# Local variables:
# # mode: shell-script
# # sh-basic-offset: 4
# # sh-indent-comment: t
# # indent-tabs-mode: nil
# # End:
# # ex: ts=4 sw=4 et filetype=sh
