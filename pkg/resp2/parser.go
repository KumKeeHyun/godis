package resp2

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/textproto"
	"strconv"
)

type Parser struct {
	r   *textproto.Reader
	cur Token
}

func NewParser(r io.Reader) *Parser {
	p := &Parser{
		r: textproto.NewReader(bufio.NewReader(r)),
	}

	return p
}

func (p *Parser) next() {
	p.cur = p.readToken()
}

func (p *Parser) readToken() Token {
	if p.cur.Type == EOF {
		return p.cur
	}
	b, err := p.r.ReadLineBytes()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Token{Type: EOF}
		}
		return Token{Type: ILLEGAL}
	}
	if len(b) == 0 {
		return Token{LITERAL, string(b)}
	}

	switch b[0] {
	case '+':
		return Token{SIMPLE_STRING, string(b[1:])}
	case '-':
		return Token{ERROR, string(b[1:])}
	case ':':
		return Token{INTEGER, string(b[1:])}
	case '$':
		return Token{BULK_STRING, string(b[1:])}
	case '*':
		return Token{ARRAY, string(b[1:])}
	default:
		return Token{LITERAL, string(b)}
	}
}

func (p *Parser) Parse() (reply Reply, err error) {
	p.next()

	switch p.cur.Type {
	case EOF:
		return nil, io.EOF
	case ILLEGAL:
		return nil, errors.New("illegal reply")
	case ARRAY:
		reply, err = p.ParseArray()
	case SIMPLE_STRING:
		reply = p.parseSimpleString()
	case ERROR:
		reply = p.parseError()
	case INTEGER:
		reply = p.parseInteger()
	case BULK_STRING:
		reply = p.parseBulkString()
	}
	return
}

func (p *Parser) parseSimpleString() Reply {
	return &SimpleStringReply{
		Value: p.cur.Literal,
	}
}

func (p *Parser) parseError() Reply {
	return &ErrorReply{
		Value: p.cur.Literal,
	}
}

func (p *Parser) parseInteger() Reply {
	i, _ := strconv.Atoi(p.cur.Literal)
	return &IntegerReply{
		Value: i,
	}
}

func (p *Parser) parseBulkString() Reply {
	l, _ := strconv.Atoi(p.cur.Literal)
	bulkStr := &BulkStringReply{Len: l}
	if l == -1 {
		return bulkStr
	}
	p.next()

	if p.cur.Type != LITERAL {
		return nil
	}

	return &BulkStringReply{
		Len:   l,
		Value: p.cur.Literal,
	}
}

func (p *Parser) ParseArray() (Reply, error) {
	l, _ := strconv.Atoi(p.cur.Literal)
	array := &ArrayReply{Len: l}
	if l == -1 {
		return array, nil
	}

	array.Value = make([]Reply, 0, l)
	for i := 0; i < l; i++ {
		reply, err := p.Parse()
		if err != nil {
			return nil, fmt.Errorf("failed to parse array in idx(%d): %v", i, err)
		}
		array.Value = append(array.Value, reply)
	}
	return array, nil
}
